using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using NUnit.Framework;
using EventStore.Core.Services;

namespace EventStore.Projections.Core.Tests.Services.projections_manager
{
    [TestFixture]
    public class when_deleting_a_persistent_projection_with_emitted_streams : TestFixtureWithProjectionCoreAndManagementServices
    {
        private string _projectionName;
        private const string _projectionStateStream = "$projections-test-projection-result";
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
                CreateWriteEvent(_projectionEmittedStreamsStream, SystemEventTypes.StreamEmittedTo, new Core.Services.Processing.EmittedStreamData { StreamId = "emitted_stream_id" }.ToJson());
            yield return
                new ProjectionManagementMessage.Command.Post(
                    new PublishEnvelope(_bus), ProjectionMode.Continuous, _projectionName,
                    ProjectionManagementMessage.RunAs.System, "JS", @"fromAll().whenAny(function(s,e){return s;});",
                    enabled: true, checkpointsEnabled: true, emitEnabled: true);
            yield return
                new ProjectionManagementMessage.Command.Disable(
                    new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System);
            yield return
                new ProjectionManagementMessage.Command.Delete(
                    new PublishEnvelope(_bus), _projectionName,
                    ProjectionManagementMessage.RunAs.System, false, false, true);
        }

        [Test, Category("v8")]
        public void a_projection_deleted_event_is_written()
        {
            Assert.AreEqual(
                "$ProjectionDeleted",
                _consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Last().Events[0].EventType);
            Assert.AreEqual(
                _projectionName,
                Helper.UTF8NoBom.GetString(_consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Last().Events[0].Data));
        }

        [Test, Category("v8")]
        public void should_have_deleted_the_emitted_streams()
        {
            Assert.IsTrue(
                _consumer.HandledMessages.OfType<ClientMessage.DeleteStream>().Any(x=>x.EventStreamId == "emitted_stream_id"));
        }

        [Test, Category("v8")]
        public void should_have_deleted_the_emitted_streams_stream()
        {
            Assert.IsTrue(
                _consumer.HandledMessages.OfType<ClientMessage.DeleteStream>().Any(x=>x.EventStreamId == _projectionEmittedStreamsStream));
        }
    }
}
