using EventStore.Core;
using EventStore.Projections.Core;

namespace EventStore.ClusterNode
{
    /// <summary>
    /// Allows a client to build a <see cref="ClusterVNode" /> for use in EventStore.ClusterNode by specifying
    /// high level options rather than using the constructor of <see cref="ClusterVNode"/> directly.
    /// </summary>
    public class ClusterVNodeBuilder : VNodeBuilder
    {
        protected ClusterVNodeBuilder()
        {
        }

        /// <summary>
        /// Returns a builder set to construct options for a single node instance
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public static ClusterVNodeBuilder AsSingleNode()
        {
            var ret = new ClusterVNodeBuilder
            {
                _clusterNodeCount = 1,
                _prepareAckCount = 1,
                _commitAckCount = 1
            };
            return ret;
        }

        /// <summary>
        /// Returns a builder set to construct options for a cluster node instance with a cluster size 
        /// </summary>
        /// <returns>A <see cref="VNodeBuilder"/> with the options set</returns>
        public static ClusterVNodeBuilder AsClusterMember(int clusterSize)
        {
            int quorumSize = clusterSize / 2;
            var ret = new ClusterVNodeBuilder
            {
                _clusterNodeCount = clusterSize,
                _prepareAckCount = quorumSize,
                _commitAckCount = quorumSize
            };
            return ret;
        }
        
        protected override void SetUpProjectionsIfNeeded()
        {
            _subsystems.Add(new ProjectionsSubsystem(_projectionsThreads, _projectionType, _startStandardProjections));
        }
    }
}
