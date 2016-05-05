using EventStore.Core.Cluster.Settings;
using EventStore.Core.TransactionLog.Chunks;
using NUnit.Framework;
using System;

namespace EventStore.Core.Tests.Common.VNodeBuilderTests
{
    [TestFixture]
    public abstract class SingleNodeScenario
    {
        protected VNodeBuilder _builder;
        protected ClusterVNode _node;
        protected ClusterVNodeSettings _settings;
        protected TFChunkDbConfig _dbConfig;

        [TestFixtureSetUp]
        public virtual void TestFixtureSetUp()
        {
            _builder = TestVNodeBuilder.AsSingleNode()
                                       .RunInMemory()
                                       .OnDefaultEndpoints();
            Given();
            _node = _builder.Build();
            _settings = ((TestVNodeBuilder)_builder).GetSettings();
            _dbConfig = ((TestVNodeBuilder)_builder).GetDbConfig();
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
        protected TFChunkDbConfig _dbConfig;
        protected int _clusterSize = 3;
        protected int _quorumSize;

        [TestFixtureSetUp]
        public virtual void TestFixtureSetUp()
        {
            _builder = TestVNodeBuilder.AsClusterMember(_clusterSize)
                                       .RunInMemory()
                                       .OnDefaultEndpoints();
            _quorumSize = _clusterSize / 2 + 1;
            Given();
            _node = _builder.Build();
            _settings = ((TestVNodeBuilder)_builder).GetSettings();
            _dbConfig = ((TestVNodeBuilder)_builder).GetDbConfig();
            Console.WriteLine(_settings);
        }

        public abstract void Given();
    }
}