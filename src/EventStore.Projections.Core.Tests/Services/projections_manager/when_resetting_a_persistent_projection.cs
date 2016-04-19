using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Services.projections_manager
{
    [TestFixture]
    public class when_resetting_a_persistent_projection : TestFixtureWithProjectionCoreAndManagementServices
    {
        private string _projectionName;
        private const string _projectionCheckpointStream = "$projections-test-projection-checkpoint";
        private const string _projectionEmittedStreamsStream = "$projections-test-projection-emittedstreams";

        protected override void Given()
        {
            _projectionName = "test-projection";
            AllWritesSucceed();
            NoOtherStreams();
        }

        protected override IEnumerable<WhenStep> When()
        {
            yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
            yield return new SystemMessage.SystemReady();
            yield return
                new ProjectionManagementMessage.Command.Post(
                    new PublishEnvelope(_bus), ProjectionMode.Continuous, _projectionName,
                    ProjectionManagementMessage.RunAs.System, "JS", @"fromAll().whenAny(function(s,e){return s;});",
                    enabled: true, checkpointsEnabled: true, emitEnabled: true);
            yield return
                new ProjectionManagementMessage.Command.Disable(
                    new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System);
            yield return
                new ProjectionManagementMessage.Command.Reset(
                    new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System);
        }

        [Test, Category("v8")]
        public void should_have_attempted_to_delete_the_emitted_streams_stream()
        {
            Assert.IsTrue(
                _consumer.HandledMessages.OfType<ClientMessage.DeleteStream>().Any(x=>x.EventStreamId == _projectionEmittedStreamsStream));
        }
        [Test, Category("v8")]
        public void should_attempt_to_delete_the_emitted_streams_stream()
        {
            _manager.Handle(
                new ProjectionManagementMessage.Command.GetStatistics(
                    new PublishEnvelope(_bus), null, _projectionName, false));

            Assert.AreEqual(1, _consumer.HandledMessages.OfType<ClientMessage.DeleteStream>().Where(x => x.EventStreamId == "$projections-test-projection-emittedstreams").Count());
        }
    }
}
