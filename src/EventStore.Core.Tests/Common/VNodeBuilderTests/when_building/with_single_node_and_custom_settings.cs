using NUnit.Framework;
using System;
using System.Net;

namespace EventStore.Core.Tests.Common.VNodeBuilderTests.when_building
{
    [TestFixture]
    [Category("Hayley")]
    public class with_log_http_requests_enabled : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.EnableLoggingOfHttpRequests();
        }

        [Test]
        public void should_turn_on_http_logging()
        {
            Assert.IsTrue(_settings.LogHttpRequests);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_worker_threads_set_to_custom_amount : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithWorkerThreads(10);
        }

        [Test]
        public void should_set_the_number_of_worker_threads()
        {
            Assert.AreEqual(10, _settings.WorkerThreads);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_stats_period_set_to_custom_amount : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithStatsPeriod(TimeSpan.FromSeconds(1));
        }

        [Test]
        public void should_set_the_stats_period()
        {
            Assert.AreEqual(1, _settings.StatsPeriod.TotalSeconds);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_histograms_enabled : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.EnableHistograms();
        }

        [Test]
        public void should_enable_histograms()
        {
            Assert.IsTrue(_settings.EnableHistograms);
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
    public class with_http_caching_disabled : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.DisableHTTPCaching();
        }

        [Test]
        public void should_disable_http_caching()
        {
            Assert.IsTrue(_settings.DisableHTTPCaching);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class without_verifying_db_hashes : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.DoNotVerifyDbHashes();
        }

        [Test]
        public void should_not_verify_db_hashes()
        {
            Assert.IsFalse(_settings.VerifyDbHash);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_min_flush_delay_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithMinFlushDelay(TimeSpan.FromMilliseconds(1200));
        }

        [Test]
        public void should_set_the_min_flush_delay()
        {
            Assert.AreEqual(1200, _settings.MinFlushDelay.TotalMilliseconds);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_scavenge_history_max_age_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithScavengeHistoryMaxAge(2);
        }

        [Test]
        public void should_set_the_scavenge_history_max_age()
        {
            Assert.AreEqual(2, _settings.ScavengeHistoryMaxAge);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_scavenge_merging_disabled : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.DisableScavengeMerging();
        }

        [Test]
        public void should_disable_scavenge_merging()
        {
            Assert.IsTrue(_settings.DisableScavengeMerging);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_max_memtable_size_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.MaximumMemoryTableSizeOf(200);
        }

        [Test]
        public void should_set_the_max_memtable_size()
        {
            Assert.AreEqual(200, _settings.MaxMemtableEntryCount);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_standard_projections_started : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.StartStandardProjections();
        }

        [Test]
        public void should_start_standard_projections()
        {
            Assert.IsTrue(_settings.StartStandardProjections);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_ignore_hard_delete_enabled : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithUnsafeIgnoreHardDelete();
        }

        [Test]
        public void should_set_ignore_hard_deletes()
        {
            Assert.IsTrue(_settings.UnsafeIgnoreHardDeletes);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_better_ordering_enabled : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithBetterOrdering();
        }

        [Test]
        public void should_set_the_min_flush_delay()
        {
            Assert.IsTrue(_settings.BetterOrdering);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_index_path_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithIndexPath("index");
        }

        [Test]
        public void should_set_the_index_path()
        {
            Assert.AreEqual("index", _settings.Index);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_prepare_timeout_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithPrepareTimeout(TimeSpan.FromMilliseconds(1234));
        }

        [Test]
        public void should_set_the_prepare_timeout()
        {
            Assert.AreEqual(1234, _settings.PrepareTimeout.TotalMilliseconds);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_commit_timeout_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithCommitTimeout(TimeSpan.FromMilliseconds(1234));
        }

        [Test]
        public void should_set_the_commit_timeout()
        {
            Assert.AreEqual(1234, _settings.CommitTimeout.TotalMilliseconds);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_internal_heartbeat_interval_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithInternalHeartbeatInterval(TimeSpan.FromMilliseconds(1234));
        }

        [Test]
        public void should_set_the_internal_heartbeat_interval()
        {
            Assert.AreEqual(1234, _settings.IntTcpHeartbeatInterval.TotalMilliseconds);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_internal_heartbeat_timeout_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithInternalHeartbeatTimeout(TimeSpan.FromMilliseconds(1234));
        }

        [Test]
        public void should_set_the_internal_heartbeat_timeout()
        {
            Assert.AreEqual(1234, _settings.IntTcpHeartbeatTimeout.TotalMilliseconds);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_external_heartbeat_interval_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithExternalHeartbeatInterval(TimeSpan.FromMilliseconds(1234));
        }

        [Test]
        public void should_set_the_external_heartbeat_interval()
        {
            Assert.AreEqual(1234, _settings.ExtTcpHeartbeatInterval.TotalMilliseconds);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_external_heartbeat_timeout_set_to_custom_value : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithExternalHeartbeatTimeout(TimeSpan.FromMilliseconds(1234));
        }

        [Test]
        public void should_set_the_external_heartbeat_timeout()
        {
            Assert.AreEqual(1234, _settings.ExtTcpHeartbeatTimeout.TotalMilliseconds);
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
        public void should_disable_stats_on_public()
        {
            Assert.IsFalse(_settings.StatsOnPublic);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_ip_endpoints_set_to_custom_values : SingleNodeScenario
    {
        private IPEndPoint _internalHttp;
        private IPEndPoint _externalHttp;
        private IPEndPoint _internalTcp;
        private IPEndPoint _externalTcp;
        public override void Given()
        {
            var baseIpAddress = IPAddress.Parse("192.168.1.15");
            _internalHttp = new IPEndPoint(baseIpAddress, 1112);
            _externalHttp = new IPEndPoint(baseIpAddress, 1113);
            _internalTcp = new IPEndPoint(baseIpAddress, 1114);
            _externalTcp = new IPEndPoint(baseIpAddress, 1115);
            _builder.WithInternalHttpOn(_internalHttp)
                    .WithExternalHttpOn(_externalHttp)
                    .WithExternalTcpOn(_externalTcp)
                    .WithInternalTcpOn(_internalTcp);
        }

        [Test]
        public void should_set_internal_http_endpoint()
        {
            Assert.AreEqual(_internalHttp, _settings.NodeInfo.InternalHttp);
        }

        [Test]
        public void should_set_external_http_endpoint()
        {
            Assert.AreEqual(_externalHttp, _settings.NodeInfo.ExternalHttp);
        }

        [Test]
        public void should_set_internal_tcp_endpoint()
        {
            Assert.AreEqual(_internalTcp, _settings.NodeInfo.InternalTcp);
        }

        [Test]
        public void should_set_external_tcp_endpoint()
        {
            Assert.AreEqual(_externalTcp, _settings.NodeInfo.ExternalTcp);
        }

        [Test]
        public void should_set_internal_http_prefixes()
        {
            var internalHttpPrefix = string.Format("http://{0}/", _internalHttp);
            CollectionAssert.AreEqual(new string[] {internalHttpPrefix}, _settings.IntHttpPrefixes);
        }

        [Test]
        public void should_set_external_http_prefixes()
        {
            var externalHttpPrefix = string.Format("http://{0}/", _externalHttp);
            CollectionAssert.AreEqual(new string[] {externalHttpPrefix}, _settings.ExtHttpPrefixes);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_custom_http_prefixes : SingleNodeScenario
    {

        private string _intPrefix;
        private string _intLoopbackPrefix;
        private string _extPrefix;
        private string _extLoopbackPrefix;

        public override void Given()
        {
            var baseIpAddress = IPAddress.Parse("192.168.1.15");
            int intPort = 1112;
            int extPort = 1113;

            var internalHttp = new IPEndPoint(baseIpAddress, intPort);
            var externalHttp = new IPEndPoint(baseIpAddress, extPort);

            _intPrefix = string.Format("http://{0}/", internalHttp);
            _intLoopbackPrefix = string.Format("http://{0}/", new IPEndPoint(IPAddress.Loopback, intPort));
            _extPrefix = string.Format("http://{0}/", externalHttp);
            _extLoopbackPrefix = string.Format("http://{0}/", new IPEndPoint(IPAddress.Loopback, extPort));

            _builder.WithInternalHttpOn(internalHttp)
                    .WithExternalHttpOn(externalHttp)
                    .AddInternalHttpPrefix(_intPrefix)
                    .AddInternalHttpPrefix(_intLoopbackPrefix)
                    .AddExternalHttpPrefix(_extPrefix)
                    .AddExternalHttpPrefix(_extLoopbackPrefix);
        }

        [Test]
        public void should_set_internal_http_prefixes()
        {
            CollectionAssert.AreEqual(new string[] {_intPrefix, _intLoopbackPrefix}, _settings.IntHttpPrefixes);
        }

        [Test]
        public void should_set_external_http_prefixes()
        {
            CollectionAssert.AreEqual(new string[] {_extPrefix, _extLoopbackPrefix}, _settings.ExtHttpPrefixes);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_ssl_enabled : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.EnableSsl();
        }

        [Test]
        public void should_set_ssl_to_enabled()
        {
            Assert.IsTrue(_settings.UseSsl);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_ssl_target_host : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.WithSslTargetHost("Host");
        }

        [Test]
        public void should_set_ssl_target_host()
        {
            Assert.AreEqual("Host", _settings.SslTargetHost);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_validate_ssl_server_enabled : SingleNodeScenario
    {
        public override void Given()
        {
            _builder.ValidateSslServer();
        }

        [Test]
        public void should_enable_validating_ssl_server()
        {
            Assert.IsTrue(_settings.SslValidateServer);
        }
    }
}