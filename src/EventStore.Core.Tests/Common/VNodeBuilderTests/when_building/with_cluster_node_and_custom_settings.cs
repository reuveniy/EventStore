using NUnit.Framework;
using System.Net;

namespace EventStore.Core.Tests.Common.VNodeBuilderTests.when_building
{
    [TestFixture]
    [Category("Hayley")]
    public class with_dns_name_set_to_custom_value : ClusterMemberScenario
    {
        public override void Given()
        {
            _builder.WithClusterDnsName("ClusterDns");
        }

        [Test]
        public void should_set_cluster_dns_name()
        {
            Assert.AreEqual("ClusterDns", _settings.ClusterDns);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_prepare_ack_count_set_higher_than_the_quorum : ClusterMemberScenario
    {
        public override void Given()
        {
            _builder.WithPrepareCount(_quorumSize + 1);
        }

        [Test]
        public void should_set_prepare_count()
        {
            Assert.AreEqual(_quorumSize + 1, _settings.PrepareAckCount);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_commit_ack_count_set_higher_than_the_quorum : ClusterMemberScenario
    {
        public override void Given()
        {
            _builder.WithCommitCount(_quorumSize + 1);
        }

        [Test]
        public void should_set_commit_count()
        {
            Assert.AreEqual(_quorumSize + 1, _settings.CommitAckCount);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_prepare_ack_count_set_lower_than_the_quorum : ClusterMemberScenario
    {
        public override void Given()
        {
            _builder.WithPrepareCount(_quorumSize - 1);
        }

        [Test]
        public void should_set_prepare_count_to_the_quorum_size()
        {
            Assert.AreEqual(_quorumSize, _settings.PrepareAckCount);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_commit_ack_count_set_lower_than_the_quorum : ClusterMemberScenario
    {
        public override void Given()
        {
            _builder.WithCommitCount(_quorumSize - 1);
        }

        [Test]
        public void should_set_commit_count_to_the_quorum_size()
        {
            Assert.AreEqual(_quorumSize, _settings.CommitAckCount);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_node_priority_set : ClusterMemberScenario
    {
        public override void Given()
        {
            _builder.WithNodePriority(5);
        }

        [Test]
        public void should_set_the_node_priority()
        {
            Assert.AreEqual(5, _settings.NodePriority);
        }
    }

    [TestFixture]
    [Category("Hayley")]
    public class with_gossip_seeds : ClusterMemberScenario
    {
    	private IPEndPoint[] _gossipSeeds;
        public override void Given()
        {
        	var baseIpAddress = IPAddress.Parse("192.168.1.15");
        	_gossipSeeds = new IPEndPoint[] { new IPEndPoint(baseIpAddress, 2112), new IPEndPoint(baseIpAddress, 3112)};
            _builder.WithGossipSeeds(_gossipSeeds);
        }

        [Test]
        public void should_set_the_gossip_seeds()
        {
        	CollectionAssert.AreEqual(_gossipSeeds, _settings.GossipSeeds);
        }
    }
}
