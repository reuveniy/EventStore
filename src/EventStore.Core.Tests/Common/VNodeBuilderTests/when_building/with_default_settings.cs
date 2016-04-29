using EventStore.Common.Options;
using EventStore.Core.Util;
using EventStore.Core.Cluster.Settings;
using EventStore.Core.Services.Monitoring;
using EventStore.Core.Authentication;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace EventStore.Core.Tests.Common.VNodeBuilderTests.when_building
{
    [TestFixture]
    public abstract class SingleNodeScenario
    {
        protected VNodeBuilder _builder;
        protected ClusterVNode _node;
        protected ClusterVNodeSettings _settings;

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            _builder = TestVNodeBuilder.AsSingleNode()
                                        .OnDefaultEndpoints();
            Given();
            _node = _builder.Build();
            _settings = ((TestVNodeBuilder)_builder).GetSettings();
            Console.WriteLine(_settings);
        }

        public abstract void Given();
    }

        [TestFixture]
    public abstract class ClusterMemberScenario
    {
        protected VNodeBuilder _builder;
        protected ClusterVNode _node;
        protected ClusterVNodeSettings _settings;
        protected int _clusterSize = 3;

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            _builder = TestVNodeBuilder.AsClusterMember(_clusterSize)
                                        .OnDefaultEndpoints();
            Given();
            _node = _builder.Build();
            _settings = ((TestVNodeBuilder)_builder).GetSettings();
            Console.WriteLine(_settings.ToString());
        }

        public abstract void Given();
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_default_settings_as_single_node : SingleNodeScenario
    {
        public override void Given()
        {
        }

        [Test]
        public void should_create_single_cluster_node()
        {
            Assert.IsNotNull(_node);
            Assert.AreEqual(1, _settings.ClusterNodeCount, "ClusterNodeCount");
            Assert.IsInstanceOf<InternalAuthenticationProviderFactory>(_settings.AuthenticationProviderFactory);
        }

        [Test]
        public void should_have_default_endpoints()
        {
            Assert.AreEqual(new IPEndPoint(IPAddress.Loopback, 1112), _settings.NodeInfo.InternalTcp);
            Assert.AreEqual(new IPEndPoint(IPAddress.Loopback, 1113), _settings.NodeInfo.ExternalTcp);
            Assert.AreEqual(new IPEndPoint(IPAddress.Loopback, 2112), _settings.NodeInfo.InternalHttp);
            Assert.AreEqual(new IPEndPoint(IPAddress.Loopback, 2113), _settings.NodeInfo.ExternalHttp);

            CollectionAssert.AreEqual(new string[] {"http://127.0.0.1:2112/", "http://localhost:2112/"}, _settings.IntHttpPrefixes);
            CollectionAssert.AreEqual(new string[] {"http://127.0.0.1:2113/", "http://localhost:2113/"}, _settings.ExtHttpPrefixes);
        }

        [Test]
        public void should_not_use_ssl()
        {
            Assert.AreEqual("n/a", _settings.Certificate == null ? "n/a" : _settings.Certificate.ToString());
            Assert.IsFalse(_settings.UseSsl);
            Assert.AreEqual("n/a", _settings.SslTargetHost == null ? "n/a" : _settings.SslTargetHost);
        }

        [Test]
        public void should_set_command_line_args_to_default_values()
        {
            Assert.AreEqual(Opts.EnableTrustedAuthDefault, _settings.EnableTrustedAuth, "EnableTrustedAuth");
            Assert.AreEqual(Opts.LogHttpRequestsDefault, _settings.LogHttpRequests, "LogHttpRequests");
            Assert.AreEqual(Opts.WorkerThreadsDefault, _settings.WorkerThreads, "WorkerThreads");
            Assert.AreEqual(Opts.DiscoverViaDnsDefault, _settings.DiscoverViaDns, "DiscoverViaDns");
            Assert.AreEqual(Opts.StatsPeriodDefault, _settings.StatsPeriod.Seconds, "StatsPeriod");
            Assert.AreEqual(Opts.HistogramEnabledDefault, _settings.EnableHistograms, "EnableHistograms");
            Assert.AreEqual(Opts.DisableHttpCachingDefault, _settings.DisableHTTPCaching, "DisableHTTPCaching");
            Assert.AreEqual(Opts.SkipDbVerifyDefault, !_settings.VerifyDbHash, "VerifyDbHash");
            Assert.AreEqual(Opts.MinFlushDelayMsDefault, _settings.MinFlushDelay.TotalMilliseconds, "MinFlushDelay");
            Assert.AreEqual(Opts.ScavengeHistoryMaxAgeDefault, _settings.ScavengeHistoryMaxAge, "ScavengeHistoryMaxAge");
            Assert.AreEqual(Opts.DisableScavengeMergeDefault, _settings.DisableScavengeMerging, "DisableScavengeMerging");
            Assert.AreEqual(Opts.AdminOnExtDefault, _settings.AdminOnPublic, "AdminOnPublic");
            Assert.AreEqual(Opts.StatsOnExtDefault, _settings.StatsOnPublic, "StatsOnPublic");
            Assert.AreEqual(Opts.GossipOnExtDefault, _settings.GossipOnPublic, "GossipOnPublic");
            Assert.AreEqual(Opts.MaxMemtableSizeDefault, _settings.MaxMemtableEntryCount, "MaxMemtableEntryCount");
            Assert.AreEqual(Opts.StartStandardProjectionsDefault, _settings.StartStandardProjections, "StartStandardProjections");
            Assert.AreEqual(Opts.UnsafeIgnoreHardDeleteDefault, _settings.UnsafeIgnoreHardDeletes, "UnsafeIgnoreHardDeletes");
            Assert.AreEqual(Opts.BetterOrderingDefault, _settings.BetterOrdering, "BetterOrdering");
            Assert.IsNullOrEmpty(_settings.Index, "IndexPath");
            Assert.AreEqual(1, _settings.PrepareAckCount, "PrepareAckCount");
            Assert.AreEqual(1, _settings.CommitAckCount, "CommitAckCount");
            Assert.AreEqual(Opts.PrepareTimeoutMsDefault, _settings.PrepareTimeout.TotalMilliseconds, "PrepareTimeout");
            Assert.AreEqual(Opts.CommitTimeoutMsDefault, _settings.CommitTimeout.TotalMilliseconds, "CommitTimeout");

            Assert.AreEqual(Opts.IntTcpHeartbeatInvervalDefault, _settings.IntTcpHeartbeatInterval.TotalMilliseconds, "IntTcpHeartbeatInterval");
            Assert.AreEqual(Opts.IntTcpHeartbeatTimeoutDefault, _settings.IntTcpHeartbeatTimeout.TotalMilliseconds, "IntTcpHeartbeatTimeout");
            Assert.AreEqual(Opts.ExtTcpHeartbeatIntervalDefault, _settings.ExtTcpHeartbeatInterval.TotalMilliseconds, "ExtTcpHeartbeatInterval");
            Assert.AreEqual(Opts.ExtTcpHeartbeatTimeoutDefault, _settings.ExtTcpHeartbeatTimeout.TotalMilliseconds, "ExtTcpHeartbeatTimeout");
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_default_settings_as_node_in_a_cluster : ClusterMemberScenario
    {
        public override void Given()
        {
        }

        [Test]
        public void should_create_single_cluster_node()
        {
            Assert.IsNotNull(_node);
            Assert.AreEqual(_clusterSize, _settings.ClusterNodeCount, "ClusterNodeCount");
            Assert.IsInstanceOf<InternalAuthenticationProviderFactory>(_settings.AuthenticationProviderFactory);
        }

        [Test]
        public void should_have_default_endpoints()
        {
            var internalTcp = new IPEndPoint(IPAddress.Loopback, 1112); 
            var externalTcp = new IPEndPoint(IPAddress.Loopback, 1113);
            var internalHttp = new IPEndPoint(IPAddress.Loopback, 2112);
            var externalHttp = new IPEndPoint(IPAddress.Loopback, 2113); 

            Assert.AreEqual(internalTcp, _settings.NodeInfo.InternalTcp);
            Assert.AreEqual(externalTcp, _settings.NodeInfo.ExternalTcp);
            Assert.AreEqual(internalHttp, _settings.NodeInfo.InternalHttp);
            Assert.AreEqual(externalHttp, _settings.NodeInfo.ExternalHttp);

            CollectionAssert.AreEqual(new string[] {"http://127.0.0.1:2112/", "http://localhost:2112/"}, _settings.IntHttpPrefixes);
            CollectionAssert.AreEqual(new string[] {"http://127.0.0.1:2113/", "http://localhost:2113/"}, _settings.ExtHttpPrefixes);

            Assert.AreEqual(internalTcp, _settings.GossipAdvertiseInfo.InternalTcp);
            Assert.AreEqual(externalTcp, _settings.GossipAdvertiseInfo.ExternalTcp);
            Assert.AreEqual(internalHttp, _settings.GossipAdvertiseInfo.InternalHttp);
            Assert.AreEqual(externalHttp, _settings.GossipAdvertiseInfo.ExternalHttp);
        }

        [Test]
        public void should_not_use_ssl()
        {
            Assert.AreEqual("n/a", _settings.Certificate == null ? "n/a" : _settings.Certificate.ToString());
            Assert.IsFalse(_settings.UseSsl);
            Assert.AreEqual("n/a", _settings.SslTargetHost == null ? "n/a" : _settings.SslTargetHost.ToString());
        }

        [Test]
        public void should_set_command_line_args_to_default_values()
        {
            Assert.AreEqual(Opts.EnableTrustedAuthDefault, _settings.EnableTrustedAuth, "EnableTrustedAuth");
            Assert.AreEqual(Opts.LogHttpRequestsDefault, _settings.LogHttpRequests, "LogHttpRequests");
            Assert.AreEqual(Opts.WorkerThreadsDefault, _settings.WorkerThreads, "WorkerThreads");
            Assert.AreEqual(Opts.DiscoverViaDnsDefault, _settings.DiscoverViaDns, "DiscoverViaDns");
            Assert.AreEqual(Opts.StatsPeriodDefault, _settings.StatsPeriod.Seconds, "StatsPeriod");
            Assert.AreEqual(Opts.HistogramEnabledDefault, _settings.EnableHistograms, "EnableHistograms");
            Assert.AreEqual(Opts.DisableHttpCachingDefault, _settings.DisableHTTPCaching, "DisableHTTPCaching");
            Assert.AreEqual(Opts.SkipDbVerifyDefault, !_settings.VerifyDbHash, "VerifyDbHash");
            Assert.AreEqual(Opts.MinFlushDelayMsDefault, _settings.MinFlushDelay.TotalMilliseconds, "MinFlushDelay");
            Assert.AreEqual(Opts.ScavengeHistoryMaxAgeDefault, _settings.ScavengeHistoryMaxAge, "ScavengeHistoryMaxAge");
            Assert.AreEqual(Opts.DisableScavengeMergeDefault, _settings.DisableScavengeMerging, "DisableScavengeMerging");
            Assert.AreEqual(Opts.AdminOnExtDefault, _settings.AdminOnPublic, "AdminOnPublic");
            Assert.AreEqual(Opts.StatsOnExtDefault, _settings.StatsOnPublic, "StatsOnPublic");
            Assert.AreEqual(Opts.GossipOnExtDefault, _settings.GossipOnPublic, "GossipOnPublic");
            Assert.AreEqual(Opts.MaxMemtableSizeDefault, _settings.MaxMemtableEntryCount, "MaxMemtableEntryCount");
            Assert.AreEqual(Opts.StartStandardProjectionsDefault, _settings.StartStandardProjections, "StartStandardProjections");
            Assert.AreEqual(Opts.UnsafeIgnoreHardDeleteDefault, _settings.UnsafeIgnoreHardDeletes, "UnsafeIgnoreHardDeletes");
            Assert.AreEqual(Opts.BetterOrderingDefault, _settings.BetterOrdering, "BetterOrdering");
            Assert.IsNullOrEmpty(_settings.Index, "IndexPath");
            Assert.AreEqual(Opts.PrepareTimeoutMsDefault, _settings.PrepareTimeout.TotalMilliseconds, "PrepareTimeout");
            Assert.AreEqual(Opts.CommitTimeoutMsDefault, _settings.CommitTimeout.TotalMilliseconds, "CommitTimeout");

            Assert.AreEqual(Opts.IntTcpHeartbeatInvervalDefault, _settings.IntTcpHeartbeatInterval.TotalMilliseconds, "IntTcpHeartbeatInterval");
            Assert.AreEqual(Opts.IntTcpHeartbeatTimeoutDefault, _settings.IntTcpHeartbeatTimeout.TotalMilliseconds, "IntTcpHeartbeatTimeout");
            Assert.AreEqual(Opts.ExtTcpHeartbeatIntervalDefault, _settings.ExtTcpHeartbeatInterval.TotalMilliseconds, "ExtTcpHeartbeatInterval");
            Assert.AreEqual(Opts.ExtTcpHeartbeatTimeoutDefault, _settings.ExtTcpHeartbeatTimeout.TotalMilliseconds, "ExtTcpHeartbeatTimeout");
        }

        [Test]
        public void should_set_commit_and_prepare_counts_to_quorum_size()
        {
            var quorumSize = _clusterSize / 2 + 1;
            Console.WriteLine("Quorum size {0}", quorumSize);
            Assert.AreEqual(quorumSize, _settings.PrepareAckCount, "PrepareAckCount");
            Assert.AreEqual(quorumSize, _settings.CommitAckCount, "CommitAckCount");
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_trusted_auth_enabled : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.EnableTrustedAuth();
        }

        [Test]
        public void should_enable_trusted_authentication()
        {
            Assert.IsTrue(_settings.EnableTrustedAuth);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_no_admin_on_public_interface : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.NoAdminOnPublicInterface();
        }

        [Test]
        public void should_disable_admin_on_public()
        {
            Assert.IsFalse(_settings.AdminOnPublic);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_no_gossip_on_public_interface : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.NoGossipOnPublicInterface();
        }

        [Test]
        public void should_disable_gossip_on_public()
        {
            Assert.IsFalse(_settings.GossipOnPublic);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_no_stats_on_public_interface : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.NoStatsOnPublicInterface();
        }

        [Test]
        public void should_disable_gossip_on_public()
        {
            Assert.IsFalse(_settings.StatsOnPublic);
        }
    }

    public class TestVNodeBuilder : VNodeBuilder
    {
        protected TestVNodeBuilder()
        {
        }

        public static TestVNodeBuilder AsSingleNode()
        {
            var ret = new TestVNodeBuilder
            {
                _clusterNodeCount = 1,
                _prepareAckCount = 1,
                _commitAckCount = 1
            };
            return ret;
        }

        public static TestVNodeBuilder AsClusterMember(int clusterSize)
        {
            int quorumSize = clusterSize / 2;
            var ret = new TestVNodeBuilder
            {
                _clusterNodeCount = clusterSize,
                _prepareAckCount = quorumSize,
                _commitAckCount = quorumSize
            };
            return ret;
        }
        
        protected override void SetUpProjectionsIfNeeded()
        {
        }

        public ClusterVNodeSettings GetSettings()
        {
            return _vNodeSettings;
        }
    }
}