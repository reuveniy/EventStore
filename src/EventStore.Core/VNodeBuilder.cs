using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using EventStore.Common.Options;
using EventStore.Common.Utils;
using EventStore.Core;
using EventStore.Core.Authentication;
using EventStore.Core.Cluster.Settings;
using EventStore.Core.Services.Gossip;
using EventStore.Core.Services.Monitoring;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.Util;
using EventStore.Core.Services.Transport.Http.Controllers;
using EventStore.Core.Data;
using EventStore.Core.Services.PersistentSubscription.ConsumerStrategy;

namespace EventStore.Core
{
    /// <summary>
    /// Allows a client to build a <see cref="ClusterVNode" /> for use with the Embedded client API by specifying
    /// high level options rather than using the constructor of <see cref="ClusterVNode"/> directly.
    /// </summary>
    public abstract class VNodeBuilder
    {
        // ReSharper disable FieldCanBeMadeReadOnly.Local - as more options are added
        protected int _chunkSize;
        protected string _dbPath;
        protected long _chunksCacheSize;
        protected bool _inMemoryDb;
        protected bool _startStandardProjections;
        protected bool _disableHTTPCaching;
        protected bool _logHttpRequests;
        protected bool _enableHistograms;

        protected IPEndPoint _internalTcp;
        protected IPEndPoint _internalSecureTcp;
        protected IPEndPoint _externalTcp;
        protected IPEndPoint _externalSecureTcp;
        protected IPEndPoint _internalHttp;
        protected IPEndPoint _externalHttp;

        protected List<string> _intHttpPrefixes;
        protected List<string> _extHttpPrefixes;
        protected bool _enableTrustedAuth;
        protected X509Certificate2 _certificate;
        protected int _workerThreads;

        protected bool _discoverViaDns;
        protected string _clusterDns;
        protected List<IPEndPoint> _gossipSeeds;

        protected TimeSpan _minFlushDelay;

        protected int _clusterNodeCount;
        protected int _prepareAckCount;
        protected int _commitAckCount;
        protected TimeSpan _prepareTimeout;
        protected TimeSpan _commitTimeout;

        protected int _nodePriority;

        protected bool _useSsl;
        protected string _sslTargetHost;
        protected bool _sslValidateServer;

        protected TimeSpan _statsPeriod;

        protected IAuthenticationProviderFactory _authenticationProviderFactory;
        protected bool _disableScavengeMerging;
        protected int _scavengeHistoryMaxAge;
        protected bool _adminOnPublic;
        protected bool _statsOnPublic;
        protected bool _gossipOnPublic;
        protected TimeSpan _gossipInterval;
        protected TimeSpan _gossipAllowedTimeDifference;
        protected TimeSpan _gossipTimeout;

        protected TimeSpan _intTcpHeartbeatTimeout;
        protected TimeSpan _intTcpHeartbeatInterval;
        protected TimeSpan _extTcpHeartbeatTimeout;
        protected TimeSpan _extTcpHeartbeatInterval;

        protected bool _skipVerifyDbHashes;
        protected int _maxMemtableSize;
        protected List<ISubsystem> _subsystems;
        protected int _clusterGossipPort;

        protected string _index;
        protected int _indexCacheDepth;
        protected bool _unsafeIgnoreHardDelete;
        protected bool _betterOrdering;
        protected ProjectionType _projectionType;
        protected int _projectionsThreads;
        // ReSharper restore FieldCanBeMadeReadOnly.Local

        protected VNodeBuilder()
        {
            _chunkSize = TFConsts.ChunkSize;
            _dbPath = Path.Combine(Path.GetTempPath(), "EmbeddedEventStore", string.Format("{0:yyyy-MM-dd_HH.mm.ss.ffffff}-EmbeddedNode", DateTime.UtcNow));
            _chunksCacheSize = TFConsts.ChunksCacheSize;
            _inMemoryDb = true;

            _externalTcp = new IPEndPoint(Opts.ExternalIpDefault, Opts.ExternalHttpPortDefault);
            _externalSecureTcp = null;
            _externalTcp = new IPEndPoint(Opts.ExternalIpDefault, Opts.ExternalHttpPortDefault);
            _externalSecureTcp = null;
            _externalHttp = new IPEndPoint(Opts.ExternalIpDefault, Opts.ExternalHttpPortDefault);
            _externalHttp = new IPEndPoint(Opts.ExternalIpDefault, Opts.ExternalHttpPortDefault);

            _intHttpPrefixes = new List<string>();
            _extHttpPrefixes = new List<string>();
            _enableTrustedAuth = Opts.EnableTrustedAuthDefault;
            _certificate = null;
            _workerThreads = Opts.WorkerThreadsDefault;

            _discoverViaDns = false;
            _clusterDns = Opts.ClusterDnsDefault;
            _gossipSeeds = new List<IPEndPoint>();

            _minFlushDelay = TimeSpan.FromMilliseconds(Opts.MinFlushDelayMsDefault);

            _clusterNodeCount = 1;
            _prepareAckCount = 1;
            _commitAckCount = 1;
            _prepareTimeout = TimeSpan.FromMilliseconds(Opts.PrepareTimeoutMsDefault);
            _commitTimeout = TimeSpan.FromMilliseconds(Opts.CommitTimeoutMsDefault);

            _nodePriority = Opts.NodePriorityDefault;

            _useSsl = Opts.UseInternalSslDefault;
            _sslTargetHost = Opts.SslTargetHostDefault;
            _sslValidateServer = Opts.SslValidateServerDefault;

            _statsPeriod = TimeSpan.FromSeconds(Opts.StatsPeriodDefault);

            _authenticationProviderFactory = new InternalAuthenticationProviderFactory();
            _disableScavengeMerging = Opts.DisableScavengeMergeDefault;
            _scavengeHistoryMaxAge = Opts.ScavengeHistoryMaxAgeDefault;
            _adminOnPublic = Opts.AdminOnExtDefault;
            _statsOnPublic = Opts.StatsOnExtDefault;
            _gossipOnPublic = Opts.GossipOnExtDefault;
            _gossipInterval = TimeSpan.FromMilliseconds(Opts.GossipIntervalMsDefault);
            _gossipAllowedTimeDifference = TimeSpan.FromMilliseconds(Opts.GossipAllowedDifferenceMsDefault);
            _gossipTimeout = TimeSpan.FromMilliseconds(Opts.GossipTimeoutMsDefault);

            _intTcpHeartbeatInterval = TimeSpan.FromMilliseconds(Opts.IntTcpHeartbeatInvervalDefault);
            _intTcpHeartbeatTimeout = TimeSpan.FromMilliseconds(Opts.IntTcpHeartbeatTimeoutDefault);
            _extTcpHeartbeatInterval = TimeSpan.FromMilliseconds(Opts.ExtTcpHeartbeatIntervalDefault);
            _extTcpHeartbeatTimeout = TimeSpan.FromMilliseconds(Opts.ExtTcpHeartbeatTimeoutDefault);

            _skipVerifyDbHashes = Opts.SkipDbVerifyDefault;
            _maxMemtableSize = Opts.MaxMemtableSizeDefault;
            _subsystems = new List<ISubsystem>();
            _clusterGossipPort = Opts.ClusterGossipPortDefault;

            _startStandardProjections = Opts.StartStandardProjectionsDefault;
            _disableHTTPCaching = Opts.DisableHttpCachingDefault;
            _logHttpRequests = Opts.LogHttpRequestsDefault;
            _enableHistograms = Opts.LogHttpRequestsDefault;
            _index = null;
            _indexCacheDepth = Opts.IndexCacheDepthDefault;
            _unsafeIgnoreHardDelete = Opts.UnsafeIgnoreHardDeleteDefault;
            _betterOrdering = Opts.BetterOrderingDefault;
        }

        /// <summary>
        /// Start standard projections.
        /// </summary>
        /// <returns></returns>
        public VNodeBuilder StartStandardProjections()
        {
            _startStandardProjections = true;
            return this;
        }

        /// <summary>
        /// Disable HTTP Caching.
        /// </summary>
        /// <returns></returns>
        public VNodeBuilder DisableHTTPCaching()
        {
            _disableHTTPCaching = true;
            return this;
        }

        /// <summary>
        /// Sets the mode and the number of threads on which to run projections.
        /// </summary>
        /// <param name="projectionType">The mode in which to run the projections system</param>
        /// <param name="numberOfThreads">The number of threads to use for projections. Defaults to 3.</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder RunProjections(ProjectionType projectionType, int numberOfThreads = Opts.ProjectionThreadsDefault)
        {
            _projectionType = projectionType;
            _projectionsThreads = numberOfThreads;
            return this;
        }

        /// <summary>
        /// Adds a custom subsystem to the builder. NOTE: This is an advanced use case that most people will never need!
        /// </summary>
        /// <param name="subsystem">The subsystem to add</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder AddCustomSubsystem(ISubsystem subsystem)
        {
            _subsystems.Add(subsystem);
            return this;
        }

        /// <summary>
        /// Returns a builder set to run in memory only
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder RunInMemory()
        {
            _inMemoryDb = true;
            _dbPath = Path.Combine(Path.GetTempPath(), "EmbeddedEventStore", string.Format("{0:yyyy-MM-dd_HH.mm.ss.ffffff}-EmbeddedNode", DateTime.UtcNow));
            return this;
        }

        /// <summary>
        /// Returns a builder set to write database files to the specified path
        /// </summary>
        /// <param name="path">The path on disk in which to write the database files</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder RunOnDisk(string path)
        {
            _inMemoryDb = false;
            _dbPath = path;
            return this;
        }

        /// <summary>
        /// Sets the default endpoints on localhost (1113 tcp, 2113 http)
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder OnDefaultEndpoints()
        {
            _internalHttp = new IPEndPoint(Opts.InternalIpDefault, 2112);
            _internalTcp = new IPEndPoint(Opts.InternalIpDefault, 1112);
            _externalHttp = new IPEndPoint(Opts.ExternalIpDefault, 2113);
            _externalTcp = new IPEndPoint(Opts.InternalIpDefault, 1113);
            return this;
        }

        /// <summary>
        /// Sets the internal http endpoint to the specified value
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithInternalHttpOn(IPEndPoint endpoint)
        {
            _internalHttp = endpoint;
            return this;
        }

        /// <summary>
        /// Sets the internal gossip port (used when using cluster dns, this should point to a known port gossip will be running on)
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithClusterGossipPort(int port)
        {
            _clusterGossipPort = port;
            return this;
        }

        /// <summary>
        /// Sets the external http endpoint to the specified value
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithExternalHttpOn(IPEndPoint endpoint)
        {
            _externalHttp = endpoint;
            return this;
        }

        /// <summary>
        /// Sets the internal tcp endpoint to the specified value
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithInternalTcpOn(IPEndPoint endpoint)
        {
            _internalTcp = endpoint;
            return this;
        }

        /// <summary>
        /// Sets the internal tcp endpoint to the specified value
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithInternalSecureTcpOn(IPEndPoint endpoint)
        {
            _internalSecureTcp = endpoint;
            return this;
        }

        /// <summary>
        /// Sets the external tcp endpoint to the specified value
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithExternalTcpOn(IPEndPoint endpoint)
        {
            _externalTcp = endpoint;
            return this;
        }

        /// <summary>
        /// Sets the external tcp endpoint to the specified value
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithExternalSecureTcpOn(IPEndPoint endpoint)
        {
            _externalSecureTcp = endpoint;
            return this;
        }

        /// <summary>
        /// Sets that SSL should be used on connections
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder EnableSsl()
        {
            _useSsl = true;
            return this;
        }

        /// <summary>
        /// Sets the target host of the server's SSL certificate. 
        /// </summary>
        /// <param name="targetHost">The target host of the server's SSL certificate</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithSslTargetHost(string targetHost)
        {
            _sslTargetHost = targetHost;
            return this;
        }

        /// <summary>
        /// Sets whether to validate that the server's certificate is trusted.  
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder ValidateSslServer()
        {
            _sslValidateServer = true;
            return this;
        }

        /// <summary>
        /// Sets the gossip seeds this node should talk to
        /// </summary>
        /// <param name="endpoints">The gossip seeds this node should try to talk to</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithGossipSeeds(params IPEndPoint[] endpoints)
        {
            _gossipSeeds.Clear();
            _gossipSeeds.AddRange(endpoints);
            _discoverViaDns = false;
            return this;
        }

        /// <summary>
        /// Sets the maximum size a memtable is allowed to reach (in count) before being moved to be a ptable
        /// </summary>
        /// <param name="size">The maximum count</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder MaximumMemoryTableSizeOf(int size)
        {
            _maxMemtableSize = size;
            return this;
        }

        /// <summary>
        /// Marks that the existing database files should not be checked for checksums on startup.
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder DoNotVerifyDbHashes()
        {
            _skipVerifyDbHashes = false;
            return this;
        }

        /// <summary>
        /// Disables gossip on the public (client) interface
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder NoGossipOnPublicInterface()
        {
            _gossipOnPublic = false;
            return this;
        }

        /// <summary>
        /// Disables the admin interface on the public (client) interface
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder NoAdminOnPublicInterface()
        {
            _adminOnPublic = false;
            return this;
        }

        /// <summary>
        /// Disables statistics screens on the public (client) interface
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder NoStatsOnPublicInterface()
        {
            _statsOnPublic = false;
            return this;
        }

        /// <summary>
        /// Marks that the existing database files should be checked for checksums on startup.
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder VerifyDbHashes()
        {
            _skipVerifyDbHashes = true;
            return this;
        }

        /// <summary>
        /// Sets the dns name used for the discovery of other cluster nodes
        /// </summary>
        /// <param name="name">The dns name the node should use to discover gossip partners</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithClusterDnsName(string name)
        {
            _clusterDns = name;
            _discoverViaDns = true;
            return this;
        }

        /// <summary>
        /// Sets the number of worker threads to use in shared threadpool
        /// </summary>
        /// <param name="count">The number of worker threads</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithWorkerThreads(int count)
        {
            _workerThreads = count;
            return this;
        }

        /// <summary>
        /// Adds a http prefix for the internal http endpoint
        /// </summary>
        /// <param name="prefix">The prefix to add</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder AddInternalHttpPrefix(string prefix)
        {
            _intHttpPrefixes.Add(prefix);
            return this;
        }

        /// <summary>
        /// Adds a http prefix for the external http endpoint
        /// </summary>
        /// <param name="prefix">The prefix to add</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder AddExternalHttpPrefix(string prefix)
        {
            _extHttpPrefixes.Add(prefix);
            return this;
        }

        /// <summary>
        /// Sets the Server SSL Certificate to be loaded from a file
        /// </summary>
        /// <param name="path">The path to the certificate file</param>
        /// <param name="password">The password for the certificate</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithServerCertificateFromFile(string path, string password)
        {
            var cert = new X509Certificate2(path, password);

            _certificate = cert;
            return this;
        }

        /// <summary>
        /// Sets the heartbeat interval for the internal network interface.
        /// </summary>
        /// <param name="heartbeatInterval">The heartbeat interval</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithInternalHeartbeatInterval(TimeSpan heartbeatInterval)
        {
            _intTcpHeartbeatInterval = heartbeatInterval;
            return this;
        }

        /// <summary>
        /// Sets the heartbeat interval for the external network interface.
        /// </summary>
        /// <param name="heartbeatInterval">The heartbeat interval</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithExternalHeartbeatInterval(TimeSpan heartbeatInterval)
        {
            _extTcpHeartbeatInterval = heartbeatInterval;
            return this;
        }

        /// <summary>
        /// Sets the heartbeat timeout for the internal network interface.
        /// </summary>
        /// <param name="heartbeatTimeout">The heartbeat timeout</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithInternalHeartbeatTimeout(TimeSpan heartbeatTimeout)
        {
            _intTcpHeartbeatTimeout = heartbeatTimeout;
            return this;
        }

        /// <summary>
        /// Sets the heartbeat timeout for the external network interface.
        /// </summary>
        /// <param name="heartbeatTimeout">The heartbeat timeout</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithExternalHeartbeatTimeout(TimeSpan heartbeatTimeout)
        {
            _extTcpHeartbeatTimeout = heartbeatTimeout;
            return this;
        }

        /// <summary>
        /// Sets the gossip interval
        /// </summary>
        /// <param name="gossipInterval">The gossip interval</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithGossipInterval(TimeSpan gossipInterval)
        {
            _gossipInterval = gossipInterval;
            return this;
        }

        /// <summary>
        /// Sets the allowed gossip time difference
        /// </summary>
        /// <param name="gossipAllowedDifference">The allowed gossip time difference</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithGossipAllowedTimeDifference(TimeSpan gossipAllowedDifference)
        {
            _gossipAllowedTimeDifference = gossipAllowedDifference;
            return this;
        }

        /// <summary>
        /// Sets the gossip timeout
        /// </summary>
        /// <param name="gossipTimeout">The gossip timeout</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithGossipTimeout(TimeSpan gossipTimeout)
        {
            _gossipTimeout = gossipTimeout;
            return this;
        }

        /// <summary>
        /// Sets the minimum flush delay 
        /// </summary>
        /// <param name="minFlushDelay">The minimum flush delay</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithMinFlushDelay(TimeSpan minFlushDelay)
        {
            _minFlushDelay = minFlushDelay;
            return this;
        }

        /// <summary>
        /// Sets the prepare timeout 
        /// </summary>
        /// <param name="prepareTimeout">The prepare timeout</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithPrepareTimeout(TimeSpan prepareTimeout)
        {
            _prepareTimeout = prepareTimeout;
            return this;
        }

        /// <summary>
        /// Sets the commit timeout 
        /// </summary>
        /// <param name="commitTimeout">The commit timeout</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithCommitTimeout(TimeSpan commitTimeout)
        {
            _commitTimeout = commitTimeout;
            return this;
        }

        /// <summary>
        /// Sets the period between statistics gathers
        /// </summary>
        /// <param name="statsPeriod">The period between statistics gathers</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithStatsPeriod(TimeSpan statsPeriod)
        {
            _statsPeriod = statsPeriod;
            return this;
        }

        /// <summary>
        /// Sets the number of nodes which must acknowledge prepares. 
        /// The minimum allowed value is one greater than half the cluster size.
        /// </summary>
        /// <param name="prepareCount">The number of nodes which must acknowledge prepares</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithPrepareCount(int prepareCount)
        {
            _prepareAckCount = prepareCount > _prepareAckCount ? prepareCount : _prepareAckCount;
            return this;
        }

        /// <summary>
        /// Sets the number of nodes which must acknowledge commits before acknowledging to a client.  
        /// The minimum allowed value is one greater than half the cluster size.
        /// </summary>
        /// <param name="commitCount">The number of nodes which must acknowledge commits</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithCommitCount(int commitCount)
        {
            _commitAckCount = commitCount > _commitAckCount ? commitCount : _commitAckCount;
            return this;
        }

        /// <summary>
        /// Sets the node priority used during master election
        /// </summary>
        /// <param name="nodePriority">The node priority used during master election</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithNodePriority(int nodePriority)
        {
            _nodePriority = nodePriority;
            return this;
        }

        /// <summary>
        /// Disables the merging of chunks when scavenge is running 
        /// </summary>
        /// <returns></returns>
        public VNodeBuilder DisableScavengeMerging()
        {
            _disableScavengeMerging = true;
            return this;
        }

        /// <summary>
        /// The number of days to keep scavenge history (Default: 30)
        /// </summary>
        /// <param name="scavengeHistoryMaxAge">The number of days to keep scavenge history</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithScavengeHistoryMaxAge(int scavengeHistoryMaxAge)
        {
            _scavengeHistoryMaxAge = scavengeHistoryMaxAge;
            return this;
        }

        /// <summary>
        /// Sets the path the index should be loaded/saved to
        /// </summary>
        /// <param name="indexPath">The index path</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithIndexPath(string indexPath)
        {
            _index = indexPath;
            return this;
        }

        /// <summary>
        /// Enable logging of Http Requests and Responses before they are processed
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder EnableLoggingOfHttpRequests()
        {
            _logHttpRequests = true;
            return this;
        }

        /// <summary>
        /// Enable the tracking of various histograms in the backend, typically only used for debugging
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder EnableHistograms()
        {
            _enableHistograms = true;
            return this;
        }

        /// <summary>
        /// Sets the depth to cache for the mid point cache in index
        /// </summary>
        /// <param name="indexCacheDepth">The index cache depth</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithIndexCacheDepth(int indexCacheDepth)
        {
            _indexCacheDepth = indexCacheDepth;
            return this;
        }

        /// <summary>
        /// Disables Hard Deletes (UNSAFE: use to remove hard deletes)
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithUnsafeIgnoreHardDelete()
        {
            _unsafeIgnoreHardDelete = true;
            return this;
        }

        /// <summary>
        ///     Enable Queue affinity on reads during write process to try to get better ordering.
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithBetterOrdering()
        {
            _betterOrdering = true;
            return this;
        }

        /// <summary>
        /// Sets the authentication provider factory to use
        /// </summary>
        /// <param name="authenticationProviderFactory">The authentication provider factory to use </param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithAuthenticationProvider(IAuthenticationProviderFactory authenticationProviderFactory)
        {
            _authenticationProviderFactory = authenticationProviderFactory;
            return this;
        }

        /// <summary>
        /// Sets the Server SSL Certificate to be loaded from a certificate store
        /// </summary>
        /// <param name="storeLocation">The location of the certificate store</param>
        /// <param name="storeName">The name of the certificate store</param>
        /// <param name="certificateSubjectName">The subject name of the certificate</param>
        /// <param name="certificateThumbprint">The thumbpreint of the certificate</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithServerCertificateFromStore(StoreLocation storeLocation, StoreName storeName, string certificateSubjectName, string certificateThumbprint)
        {
            var store = new X509Store(storeName, storeLocation);

            try
            {
                store.Open(OpenFlags.OpenExistingOnly);
            }
            catch (Exception exc)
            {
                throw new Exception(string.Format("Could not open certificate store '{0}' in location {1}'.", storeName, storeLocation), exc);
            }

            if (!string.IsNullOrWhiteSpace(certificateThumbprint))
            {
                var certificates = store.Certificates.Find(X509FindType.FindByThumbprint, certificateThumbprint, true);
                if (certificates.Count == 0)
                    throw new Exception(string.Format("Could not find valid certificate with thumbprint '{0}'.", certificateThumbprint));

                //Can this even happen?
                if (certificates.Count > 1)
                    throw new Exception(string.Format("Could not determine a unique certificate from thumbprint '{0}'.", certificateThumbprint));

                _certificate = certificates[0];
                return this;
            }

            if (!string.IsNullOrWhiteSpace(certificateSubjectName))
            {
                var certificates = store.Certificates.Find(X509FindType.FindBySubjectName, certificateSubjectName, true);
                if (certificates.Count == 0)
                    throw new Exception(string.Format("Could not find valid certificate with thumbprint '{0}'.", certificateThumbprint));

                //Can this even happen?
                if (certificates.Count > 1)
                    throw new Exception(string.Format("Could not determine a unique certificate from thumbprint '{0}'.", certificateThumbprint));

                _certificate = certificates[0];
                return this;
            }

            throw new ArgumentException("No thumbprint or subject name was specified for a certificate, but a certificate store was specified.");
        }


        /// <summary>
        /// Sets the transaction file chunk size. Default is <see cref="TFConsts.ChunkSize"/>
        /// </summary>
        /// <param name="chunkSize">The size of the chunk, in bytes</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithTfChunkSize(int chunkSize)
        {
            _chunkSize = chunkSize;

            return this;
        }

        /// <summary>
        /// Sets the transaction file chunk cache size. Default is <see cref="TFConsts.ChunksCacheSize"/>
        /// </summary>
        /// <param name="chunksCacheSize">The size of the cache</param>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public VNodeBuilder WithTfChunksCacheSize(long chunksCacheSize)
        {
            _chunksCacheSize = chunksCacheSize;

            return this;
        }

        private void EnsureHttpPrefixes()
        {
            if (_intHttpPrefixes == null || _intHttpPrefixes.IsEmpty())
                _intHttpPrefixes = new List<string>(new[] { _externalHttp.ToHttpUrl() });
            if (_intHttpPrefixes == null || _intHttpPrefixes.IsEmpty())
                _intHttpPrefixes = new List<string>(new[] { _externalHttp.ToHttpUrl() });

            if (!Runtime.IsMono)
                return;

            if (!_intHttpPrefixes.Contains(x => x.Contains("localhost")) && Equals(_internalHttp.Address, IPAddress.Loopback))
            {
                _intHttpPrefixes.Add(string.Format("http://localhost:{0}/", _internalHttp.Port));
            }
            if (!_extHttpPrefixes.Contains(x => x.Contains("localhost")) && Equals(_externalHttp.Address, IPAddress.Loopback))
            {
                _extHttpPrefixes.Add(string.Format("http://localhost:{0}/", _externalHttp.Port));
            }
        }

        protected abstract void SetUpProjectionsIfNeeded();

        /// <summary>
        /// Converts an <see cref="VNodeBuilder"/> to a <see cref="ClusterVNode"/>.
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static implicit operator ClusterVNode(VNodeBuilder builder)
        {
            return builder.Build();
        }

        /// <summary>
        /// Converts an <see cref="VNodeBuilder"/> to a <see cref="ClusterVNode"/>.
        /// </summary>
        /// <param name="options">The options with which to build the infoController</param>
        public ClusterVNode Build(IOptions options = null, StatsStorage statsStorage = StatsStorage.Stream, IPersistentSubscriptionConsumerStrategyFactory[] consumerStrategies = null)
        {
            EnsureHttpPrefixes();
            SetUpProjectionsIfNeeded();

            var dbConfig = CreateDbConfig(_chunkSize, _dbPath, _chunksCacheSize,
                    _inMemoryDb);
            var db = new TFChunkDb(dbConfig);

            var vNodeSettings = new ClusterVNodeSettings(Guid.NewGuid(),
                    0,
                    _internalTcp,
                    _internalSecureTcp,
                    _externalTcp,
                    _externalSecureTcp,
                    _internalHttp,
                    _externalHttp,
                    new GossipAdvertiseInfo(_internalTcp, _internalSecureTcp, _externalTcp, _externalSecureTcp, _internalHttp, _externalHttp),
                    _intHttpPrefixes.ToArray(),
                    _extHttpPrefixes.ToArray(),
                    _enableTrustedAuth,
                    _certificate,
                    _workerThreads,
                    _discoverViaDns,
                    _clusterDns,
                    _gossipSeeds.ToArray(),
                    _minFlushDelay,
                    _clusterNodeCount,
                    _prepareAckCount,
                    _commitAckCount,
                    _prepareTimeout,
                    _commitTimeout,
                    _useSsl,
                    _sslTargetHost,
                    _sslValidateServer,
                    _statsPeriod,
                    statsStorage,
                    _nodePriority,
                    _authenticationProviderFactory,
                    _disableScavengeMerging,
                    _scavengeHistoryMaxAge,
                    _adminOnPublic,
                    _statsOnPublic,
                    _gossipOnPublic,
                    _gossipInterval,
                    _gossipAllowedTimeDifference,
                    _gossipTimeout,
                    _intTcpHeartbeatTimeout,
                    _intTcpHeartbeatInterval,
                    _extTcpHeartbeatTimeout,
                    _extTcpHeartbeatInterval,
                    !_skipVerifyDbHashes,
                    _maxMemtableSize,
                    _startStandardProjections,
                    _disableHTTPCaching,
                    _logHttpRequests,
                    _index,
                    _enableHistograms,
                    _indexCacheDepth,
                    consumerStrategies,
                    _unsafeIgnoreHardDelete,
                    _betterOrdering);
            var infoController = new InfoController(options, _projectionType);
            return new ClusterVNode(db, vNodeSettings, GetGossipSource(), infoController, _subsystems.ToArray());
        }


        private IGossipSeedSource GetGossipSource()
        {
            IGossipSeedSource gossipSeedSource;
            if (_discoverViaDns)
            {
                gossipSeedSource = new DnsGossipSeedSource(_clusterDns, _clusterGossipPort);
            }
            else
            {
                if ((_gossipSeeds == null || _gossipSeeds.Count == 0) && _clusterNodeCount > 1)
                {
                    throw new Exception("DNS discovery is disabled, but no gossip seed endpoints have been specified. "
                            + "Specify gossip seeds");
                }

                if (_gossipSeeds == null)
                    throw new ApplicationException("Gossip seeds cannot be null");
                gossipSeedSource = new KnownEndpointGossipSeedSource(_gossipSeeds.ToArray());
            }
            return gossipSeedSource;
        }

        private static TFChunkDbConfig CreateDbConfig(int chunkSize, string dbPath, long chunksCacheSize, bool inMemDb)
        {
            ICheckpoint writerChk;
            ICheckpoint chaserChk;
            ICheckpoint epochChk;
            ICheckpoint truncateChk;
            if (inMemDb)
            {
                writerChk = new InMemoryCheckpoint(Checkpoint.Writer);
                chaserChk = new InMemoryCheckpoint(Checkpoint.Chaser);
                epochChk = new InMemoryCheckpoint(Checkpoint.Epoch, initValue: -1);
                truncateChk = new InMemoryCheckpoint(Checkpoint.Truncate, initValue: -1);
            }
            else
            {
                var writerCheckFilename = Path.Combine(dbPath, Checkpoint.Writer + ".chk");
                var chaserCheckFilename = Path.Combine(dbPath, Checkpoint.Chaser + ".chk");
                var epochCheckFilename = Path.Combine(dbPath, Checkpoint.Epoch + ".chk");
                var truncateCheckFilename = Path.Combine(dbPath, Checkpoint.Truncate + ".chk");
                if (Runtime.IsMono)
                {
                    writerChk = new FileCheckpoint(writerCheckFilename, Checkpoint.Writer, cached: true);
                    chaserChk = new FileCheckpoint(chaserCheckFilename, Checkpoint.Chaser, cached: true);
                    epochChk = new FileCheckpoint(epochCheckFilename, Checkpoint.Epoch, cached: true, initValue: -1);
                    truncateChk = new FileCheckpoint(truncateCheckFilename, Checkpoint.Truncate, cached: true, initValue: -1);
                }
                else
                {
                    writerChk = new MemoryMappedFileCheckpoint(writerCheckFilename, Checkpoint.Writer, cached: true);
                    chaserChk = new MemoryMappedFileCheckpoint(chaserCheckFilename, Checkpoint.Chaser, cached: true);
                    epochChk = new MemoryMappedFileCheckpoint(epochCheckFilename, Checkpoint.Epoch, cached: true, initValue: -1);
                    truncateChk = new MemoryMappedFileCheckpoint(truncateCheckFilename, Checkpoint.Truncate, cached: true, initValue: -1);
                }
            }
            var nodeConfig = new TFChunkDbConfig(dbPath,
                    new VersionedPatternFileNamingStrategy(dbPath, "chunk-"),
                    chunkSize,
                    chunksCacheSize,
                    writerChk,
                    chaserChk,
                    epochChk,
                    truncateChk,
                    inMemDb);
            return nodeConfig;
        }
    }
}
