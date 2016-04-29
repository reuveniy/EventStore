﻿using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Net;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using EventStore.Common.Exceptions;
using EventStore.Common.Options;
using EventStore.Common.Utils;
using EventStore.Core;
using EventStore.Core.Authentication;
using EventStore.Core.Cluster.Settings;
using EventStore.Core.PluginModel;
using EventStore.Core.Services.Gossip;
using EventStore.Core.Services.Monitoring;
using EventStore.Core.Services.Transport.Http.Controllers;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.Util;
using System.Net.NetworkInformation;
using EventStore.Core.Data;
using EventStore.Core.Services.PersistentSubscription;
using EventStore.Core.Services.PersistentSubscription.ConsumerStrategy;

namespace EventStore.ClusterNode
{
    public class Program : ProgramBase<ClusterNodeOptions>
    {
        private ClusterVNode _node;
        private Projections.Core.ProjectionsSubsystem _projections;
        private ExclusiveDbLock _dbLock;
        private ClusterNodeMutex _clusterNodeMutex;

        public static int Main(string[] args)
        {
            var p = new Program();
            return p.Run(args);
        }

        protected override string GetLogsDirectory(ClusterNodeOptions options)
        {
            return options.Log;
        }

        protected override string GetComponentName(ClusterNodeOptions options)
        {
            return string.Format("{0}-{1}-cluster-node", options.ExtIp, options.ExtHttpPort);
        }

        protected override void PreInit(ClusterNodeOptions options)
        {
            base.PreInit(options);

            if (options.Db.StartsWith("~") && !options.Force){
                throw new ApplicationInitializationException("The given database path starts with a '~'. We don't expand '~'. You can use --force to override this error.");
            }
            if (options.Log.StartsWith("~") && !options.Force){
                throw new ApplicationInitializationException("The given log path starts with a '~'. We don't expand '~'. You can use --force to override this error.");
            }
            if (options.GossipSeed.Length > 1 && options.ClusterSize == 1){
                throw new ApplicationInitializationException("The given ClusterSize is set to 1 but GossipSeeds are multiple. We will never be able to sync up with this configuration.");
            }

            //Never seen this problem occur on the .NET framework
            if (!Runtime.IsMono)
                return;

            //0 indicates we should leave the machine defaults alone
            if (options.MonoMinThreadpoolSize == 0)
                return;

            //Change the number of worker threads to be higher if our own setting
            // is higher than the current value
            int minWorkerThreads, minIocpThreads;
            ThreadPool.GetMinThreads(out minWorkerThreads, out minIocpThreads);

            if (minWorkerThreads >= options.MonoMinThreadpoolSize)
                return;

            if (!ThreadPool.SetMinThreads(options.MonoMinThreadpoolSize, minIocpThreads))
                Log.Error("Cannot override the minimum number of Threadpool threads (machine default: {0}, specified value: {1})", minWorkerThreads, options.MonoMinThreadpoolSize);
        }

        protected override void Create(ClusterNodeOptions opts)
        {
            var dbPath = opts.Db;

            if (!opts.MemDb)
            {
                _dbLock = new ExclusiveDbLock(dbPath);
                if (!_dbLock.Acquire())
                    throw new Exception(string.Format("Couldn't acquire exclusive lock on DB at '{0}'.", dbPath));
            }
            _clusterNodeMutex = new ClusterNodeMutex();
            if (!_clusterNodeMutex.Acquire())
                throw new Exception(string.Format("Couldn't acquire exclusive Cluster Node mutex '{0}'.", _clusterNodeMutex.MutexName));

            var dbConfig = CreateDbConfig(dbPath, opts.CachedChunks, opts.ChunksCacheSize, opts.MemDb);
            FileStreamExtensions.ConfigureFlush(disableFlushToDisk: opts.UnsafeDisableFlushToDisk);
            var db = new TFChunkDb(dbConfig);

            if (!opts.DiscoverViaDns && opts.GossipSeed.Length == 0)
            {
                if (opts.ClusterSize == 1)
                {
                    Log.Info("DNS discovery is disabled, but no gossip seed endpoints have been specified. Since"
                            + "the cluster size is set to 1, this may be intentional. Gossip seeds can be specified"
                            + "seeds using the --gossip-seed command line option.");
                }
            }


            // TODO: expose instance ID Log.Info("{0,-25} {1}", "INSTANCE ID:", _node..NodeInfo.InstanceId);
            Log.Info("{0,-25} {1}", "DATABASE:", db.Config.Path);
            Log.Info("{0,-25} {1} (0x{1:X})", "WRITER CHECKPOINT:", db.Config.WriterCheckpoint.Read());
            Log.Info("{0,-25} {1} (0x{1:X})", "CHASER CHECKPOINT:", db.Config.ChaserCheckpoint.Read());
            Log.Info("{0,-25} {1} (0x{1:X})", "EPOCH CHECKPOINT:", db.Config.EpochCheckpoint.Read());
            Log.Info("{0,-25} {1} (0x{1:X})", "TRUNCATE CHECKPOINT:", db.Config.TruncateCheckpoint.Read());

            var runProjections = opts.RunProjections;
            var enabledNodeSubsystems = runProjections >= ProjectionType.System
                ? new[] { NodeSubsystems.Projections }
            : new NodeSubsystems[0];
            _projections = new Projections.Core.ProjectionsSubsystem(opts.ProjectionThreads, opts.RunProjections, opts.StartStandardProjections);
            _node = BuildNode(opts, _projections);
            RegisterWebControllers(enabledNodeSubsystems, opts);
        }

        private void RegisterWebControllers(NodeSubsystems[] enabledNodeSubsystems, ClusterNodeOptions options)
        {
            if (_node.InternalHttpService != null)
            {
                _node.InternalHttpService.SetupController(new ClusterWebUiController(_node.MainQueue, enabledNodeSubsystems));
            }
            if (options.AdminOnExt)
            {
                _node.ExternalHttpService.SetupController(new ClusterWebUiController(_node.MainQueue, enabledNodeSubsystems));
            }
        }

        private static int GetQuorumSize(int clusterSize)
        {
            if (clusterSize == 1) return 1;
            return clusterSize / 2 + 1;
        }

        private static ClusterVNode BuildNode(ClusterNodeOptions options, ISubsystem projections)
        {
            if (options.UseInternalSsl)
            {
                if (ReferenceEquals(options.SslTargetHost, Opts.SslTargetHostDefault)) throw new Exception("No SSL target host specified.");
                if (options.IntSecureTcpPort > 0) throw new Exception("Usage of internal secure communication is specified, but no internal secure endpoint is specified!");
            }
            var quorumSize = GetQuorumSize(options.ClusterSize);

            IPAddress intIpAddressToAdvertise = options.IntIpAdvertiseAs ?? options.IntIp;
            IPAddress extIpAddressToAdvertise = options.ExtIpAdvertiseAs ?? options.ExtIp;

            var additionalIntHttpPrefixes = new List<string>(options.IntHttpPrefixes);
            var additionalExtHttpPrefixes = new List<string>(options.ExtHttpPrefixes);

            if ((options.IntIp.Equals(IPAddress.Parse("0.0.0.0")) ||
                options.ExtIp.Equals(IPAddress.Parse("0.0.0.0"))) && options.AddInterfacePrefixes)
            {
                IPAddress nonLoopbackAddress = GetNonLoopbackAddress();
                IPAddress addressToAdvertise = options.ClusterSize > 1 ? nonLoopbackAddress : IPAddress.Loopback;

                if(options.IntIp.Equals(IPAddress.Parse("0.0.0.0"))){
                    intIpAddressToAdvertise = options.IntIpAdvertiseAs ?? addressToAdvertise;
                    additionalIntHttpPrefixes.Add(String.Format("http://*:{0}/", options.IntHttpPort));
                }
                if(options.ExtIp.Equals(IPAddress.Parse("0.0.0.0"))){
                    extIpAddressToAdvertise = options.ExtIpAdvertiseAs ?? addressToAdvertise;
                    additionalExtHttpPrefixes.Add(String.Format("http://*:{0}/", options.ExtHttpPort));
                }
            }
            else if (options.AddInterfacePrefixes)
            {
                additionalIntHttpPrefixes.Add(String.Format("http://{0}:{1}/", options.IntIp, options.IntHttpPort));
                if(options.IntIp.Equals(IPAddress.Loopback)){
                    additionalIntHttpPrefixes.Add(String.Format("http://localhost:{0}/", options.IntHttpPort));
                }
                additionalExtHttpPrefixes.Add(String.Format("http://{0}:{1}/", options.ExtIp, options.ExtHttpPort));
                if(options.ExtIp.Equals(IPAddress.Loopback)){
                    additionalExtHttpPrefixes.Add(String.Format("http://localhost:{0}/", options.ExtHttpPort));
                }
            }

            var intHttpPrefixes = additionalIntHttpPrefixes.ToArray();
            var extHttpPrefixes = additionalExtHttpPrefixes.ToArray();

            var intTcpPort = options.IntTcpPortAdvertiseAs > 0 ? options.IntTcpPortAdvertiseAs : options.IntTcpPort;
            var intTcpEndPoint = new IPEndPoint(intIpAddressToAdvertise, intTcpPort);
            var intSecureTcpEndPoint = options.IntSecureTcpPort > 0 ? new IPEndPoint(intIpAddressToAdvertise, options.IntSecureTcpPort) : null;

            var extTcpPort = options.ExtTcpPortAdvertiseAs > 0 ? options.ExtTcpPortAdvertiseAs : options.ExtTcpPort;
            var extTcpEndPoint = new IPEndPoint(extIpAddressToAdvertise, extTcpPort);
            var extSecureTcpEndPoint = options.ExtSecureTcpPort > 0 ? new IPEndPoint(extIpAddressToAdvertise, options.ExtSecureTcpPort) : null;

            var intHttpPort = options.IntHttpPortAdvertiseAs > 0 ? options.IntHttpPortAdvertiseAs : options.IntHttpPort;
            var extHttpPort = options.ExtHttpPortAdvertiseAs > 0 ? options.ExtHttpPortAdvertiseAs : options.ExtHttpPort;

            var intHttpEndPoint = new IPEndPoint(intIpAddressToAdvertise, intHttpPort);
            var extHttpEndPoint = new IPEndPoint(extIpAddressToAdvertise, extHttpPort);

            var prepareCount = options.PrepareCount > quorumSize ? options.PrepareCount : quorumSize;
            Log.Info("Quorum size set to " + prepareCount);

            VNodeBuilder builder;
            if(options.ClusterSize > 1) {
                builder = VNodeBuilder.AsClusterMember(options.ClusterSize);
            } else {
                builder = VNodeBuilder.AsSingleNode();
            }

            builder.WithInternalTcpOn(intTcpEndPoint)
                        .WithInternalSecureTcpOn(intSecureTcpEndPoint)
                        .WithExternalTcpOn(extTcpEndPoint)
                        .WithExternalSecureTcpOn(extSecureTcpEndPoint)
                        .WithInternalHttpOn(intHttpEndPoint)
                        .WithExternalHttpOn(extHttpEndPoint)
                        .WithWorkerThreads(options.WorkerThreads)
                        .WithInternalHeartbeatTimeout(TimeSpan.FromMilliseconds(options.IntTcpHeartbeatTimeout))
                        .WithInternalHeartbeatInterval(TimeSpan.FromMilliseconds(options.IntTcpHeartbeatInterval))
                        .WithExternalHeartbeatTimeout(TimeSpan.FromMilliseconds(options.ExtTcpHeartbeatTimeout))
                        .WithExternalHeartbeatInterval(TimeSpan.FromMilliseconds(options.ExtTcpHeartbeatInterval))
                        .MaximumMemoryTableSizeOf(options.MaxMemTableSize)
                        .WithGossipInterval(TimeSpan.FromMilliseconds(options.GossipIntervalMs))
                        .WithGossipAllowedTimeDifference(TimeSpan.FromMilliseconds(options.GossipAllowedDifferenceMs))
                        .WithGossipTimeout(TimeSpan.FromMilliseconds(options.GossipTimeoutMs))
                        .WithMinFlushDelay(TimeSpan.FromMilliseconds(options.MinFlushDelayMs))
                        .WithPrepareTimeout(TimeSpan.FromMilliseconds(options.PrepareTimeoutMs))
                        .WithCommitTimeout(TimeSpan.FromMilliseconds(options.CommitTimeoutMs))
                        .WithStatsPeriod(TimeSpan.FromSeconds(options.StatsPeriodSec))
                        .WithPrepareCount(options.PrepareCount)
                        .WithCommitCount(options.CommitCount)
                        .WithNodePriority(options.NodePriority)
                        .WithScavengeHistoryMaxAge(options.ScavengeHistoryMaxAge)
                        .WithIndexPath(options.Index)
                        .WithIndexCacheDepth(options.IndexCacheDepth)
                        .AddCustomSubsystem(projections)
                        .WithSslTargetHost(options.SslTargetHost);

            if(options.DiscoverViaDns)
                builder.WithClusterDnsName(options.ClusterDns);
            else
                builder.WithGossipSeeds(options.GossipSeed);

            foreach(var prefix in intHttpPrefixes) {
                builder.AddInternalHttpPrefix(prefix);
            }
            foreach(var prefix in extHttpPrefixes) {
                builder.AddExternalHttpPrefix(prefix);
            }
            
            if(options.StartStandardProjections)
                builder.StartStandardProjections();
            if(options.DisableHTTPCaching)
                builder.DisableHTTPCaching();
            if(options.DisableScavengeMerging)
                builder.DisableScavengeMerging();
            if(options.LogHttpRequests)
                builder.EnableLoggingOfHttpRequests();
            if(options.EnableHistograms)
                builder.EnableHistograms();
            if(options.UnsafeIgnoreHardDelete)
                builder.WithUnsafeIgnoreHardDelete();
            if(options.BetterOrdering)
                builder.WithBetterOrdering();
            if(options.SslValidateServer)
                builder.ValidateSslServer();
            if(options.UseInternalSsl)
                builder.EnableSsl();
            if(!options.AdminOnExt)
                builder.NoAdminOnPublicInterface();
            if(!options.StatsOnExt)
                builder.NoStatsOnPublicInterface();
            if(!options.GossipOnExt)
                builder.NoGossipOnPublicInterface();
            if(options.SkipDbVerify)
                builder.DoNotVerifyDbHashes();

            if (options.IntSecureTcpPort > 0 || options.ExtSecureTcpPort > 0)
            {
                if (options.CertificateStoreName.IsNotEmptyString())
                {
                StoreLocation location;
                if (!Enum.TryParse(options.CertificateStoreLocation, out location))
                    throw new Exception(string.Format("Could not find certificate store location '{0}'", options.CertificateStoreLocation));

                StoreName name;
                if (!Enum.TryParse(options.CertificateStoreName, out name))
                    throw new Exception(string.Format("Could not find certificate store name '{0}'", options.CertificateStoreName));

                    builder.WithServerCertificateFromStore(location, name, options.CertificateSubjectName, options.CertificateThumbprint);
                }
                else if (options.CertificateFile.IsNotEmptyString())
                {
                    builder.WithServerCertificateFromFile(options.CertificateFile, options.CertificatePassword);
                }
                else
                    throw new Exception("No server certificate specified.");
            }

            var authenticationConfig = String.IsNullOrEmpty(options.AuthenticationConfig) ? options.Config : options.AuthenticationConfig;
            var plugInContainer = FindPlugins();
            var authenticationProviderFactory = GetAuthenticationProviderFactory(options.AuthenticationType, authenticationConfig, plugInContainer);
            var consumerStrategyFactories = GetPlugInConsumerStrategyFactories(plugInContainer);
            builder.WithAuthenticationProvider(authenticationProviderFactory);
            
            return builder.Build(options, StatsStorage.StreamAndCsv, consumerStrategyFactories);
        }

        private static IPAddress GetNonLoopbackAddress(){
            foreach (var adapter in NetworkInterface.GetAllNetworkInterfaces())
            {
                foreach (UnicastIPAddressInformation address in adapter.GetIPProperties().UnicastAddresses)
                {
                    if (address.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                    {
                        if (!IPAddress.IsLoopback(address.Address))
                        {
                            return address.Address;
                        }
                    }
                }
            }
            return null;
        }

        private static IPersistentSubscriptionConsumerStrategyFactory[] GetPlugInConsumerStrategyFactories(CompositionContainer plugInContainer)
        {
            var allPlugins = plugInContainer.GetExports<IPersistentSubscriptionConsumerStrategyPlugin>();

            var strategyFactories = new List<IPersistentSubscriptionConsumerStrategyFactory>();

            foreach (var potentialPlugin in allPlugins)
            {
                try
                {
                    var plugin = potentialPlugin.Value;
                    Log.Info("Loaded consumer strategy plugin: {0} version {1}.", plugin.Name, plugin.Version);
                    strategyFactories.Add(plugin.GetConsumerStrategyFactory());
                }
                catch (CompositionException ex)
                {
                    Log.ErrorException(ex, "Error loading consumer strategy plugin.");
                }
            }

            return strategyFactories.ToArray();
        }

        private static IAuthenticationProviderFactory GetAuthenticationProviderFactory(string authenticationType, string authenticationConfigFile, CompositionContainer plugInContainer)
        {
            var potentialPlugins = plugInContainer.GetExports<IAuthenticationPlugin>();

            var authenticationTypeToPlugin = new Dictionary<string, Func<IAuthenticationProviderFactory>> {
                { "internal", () => new InternalAuthenticationProviderFactory() }
            };

            foreach (var potentialPlugin in potentialPlugins)
            {
                try
                {
                    var plugin = potentialPlugin.Value;
                    var commandLine = plugin.CommandLineName.ToLowerInvariant();
                    Log.Info("Loaded authentication plugin: {0} version {1} (Command Line: {2})", plugin.Name, plugin.Version, commandLine);
                    authenticationTypeToPlugin.Add(commandLine, () => plugin.GetAuthenticationProviderFactory(authenticationConfigFile));
                }
                catch (CompositionException ex)
                {
                    Log.ErrorException(ex, "Error loading authentication plugin.");
                }
            }

            Func<IAuthenticationProviderFactory> factory;
            if (!authenticationTypeToPlugin.TryGetValue(authenticationType.ToLowerInvariant(), out factory))
            {
                throw new ApplicationInitializationException(string.Format("The authentication type {0} is not recognised. If this is supposed " +
                            "to be provided by an authentication plugin, confirm the plugin DLL is located in {1}.\n" +
                            "Valid options for authentication are: {2}.", authenticationType, Locations.PluginsDirectory, string.Join(", ", authenticationTypeToPlugin.Keys)));
            }

            return factory();
        }

        private static CompositionContainer FindPlugins()
        {
            var catalog = new AggregateCatalog();

            catalog.Catalogs.Add(new AssemblyCatalog(typeof (Program).Assembly));

            if (Directory.Exists(Locations.PluginsDirectory))
            {
                Log.Info("Plugins path: {0}", Locations.PluginsDirectory);
                catalog.Catalogs.Add(new DirectoryCatalog(Locations.PluginsDirectory));
            }
            else
            {
                Log.Info("Cannot find plugins path: {0}", Locations.PluginsDirectory);
            }

            return new CompositionContainer(catalog);
        }

        protected override void Start()
        {
            _node.Start();
        }

        public override void Stop()
        {
            _node.StopNonblocking(true, true);
        }

        protected override void OnProgramExit()
        {
            base.OnProgramExit();

            if (_dbLock != null && _dbLock.IsAcquired)
                _dbLock.Release();
        }
    }
}
