using System;
using EventStore.Core.Helpers;
using EventStore.Core.Data;
using EventStore.Core.Services;
using System.Collections.Generic;
using EventStore.Common.Utils;
using EventStore.Core.Services.UserManagement;

namespace EventStore.Projections.Core.Services.Processing
{
    public class CoreProjectionEmittedStreamsWriter
    {
        private string _emittedStreamsStreamName;
        private IODispatcher _ioDispatcher;
        private HashSet<string> _emittedStreams = new HashSet<string>();

        public CoreProjectionEmittedStreamsWriter(IODispatcher ioDispatcher, string emittedStreamsStreamName)
        {
            _ioDispatcher = ioDispatcher;
            _emittedStreamsStreamName = emittedStreamsStreamName;
            ReadEmittedStreamsStream(_emittedStreamsStreamName, 0, 1);
        }

        private void ReadEmittedStreamsStream(string streamName, int fromPosition, int count)
        {
            _ioDispatcher.ReadForward(streamName, fromPosition, count, false, SystemAccount.Principal, (m) =>
            {
                if (m.Result == ReadStreamResult.Success)
                {
                    var evt = m.Events[0].Event.Data.ParseJson<EmittedStreamData>();
                    if (!_emittedStreams.Contains(evt.StreamId))
                    {
                        _emittedStreams.Add(evt.StreamId);
                    }
                }
                if (!m.IsEndOfStream)
                {
                    ReadEmittedStreamsStream(streamName, m.NextEventNumber, 1);
                }
            });
        }

        internal void Handle(CoreProjectionProcessingMessage.EmittedStreamWriteCompleted message)
        {
            if (_emittedStreams.Contains(message.StreamId)) return;
            var emittedStreamEvent = new Event(Guid.NewGuid(), SystemEventTypes.StreamEmittedTo, true, new EmittedStreamData { StreamId = message.StreamId }.ToJsonBytes(), null);
            _ioDispatcher.WriteEvent(_emittedStreamsStreamName, ExpectedVersion.Any, emittedStreamEvent, SystemAccount.Principal, x =>
            {
                _emittedStreams.Add(message.StreamId);
            });
        }
    }

    public class EmittedStreamData
    {
        public string StreamId { get; set; }
    }
}
