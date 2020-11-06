//******************************************************************************************************
//  SubscriberHandler.cs - Gbtc
//
//  Copyright © 2020, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
//  file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  11/05/2020 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

using sttp;
using System;
using System.Collections.Generic;
using System.Linq;

namespace STTP2HIDS
{
    public class SubscriberHandler : SubscriberInstance
    {
        private ulong m_processCount;

        public Action<string>? HandleStatusMessage { get; set; }

        public Action<string>? HandleErrorMessage { get; set; }

        public Action<Dictionary<Guid, string>>? HandleReceivedMetadata { get; set; }

        public Action<Measurement>? HandleReceivedMeasurement { get; set; }

        protected override void StatusMessage(string message) => 
            HandleStatusMessage?.Invoke($"[sttp] {message}");

        protected override void ErrorMessage(string message) =>
            HandleErrorMessage?.Invoke($"[sttp] ERROR: {message}");

        protected override void DataStartTime(DateTime startTime)
        {
            // This reports timestamp of very first received measurement (if useful)
            StatusMessage($"Received first measurement at timestamp {startTime:yyyy-MM-dd HH:mm:ss.fff}");
        }

        protected override void ReceivedMetadata(ByteBuffer payload)
        {
            StatusMessage($"Received {payload.Count:N0} bytes of metadata, parsing...");
            base.ReceivedMetadata(payload);
        }

        protected override void ParsedMetadata()
        {
            StatusMessage("Metadata successfully parsed.");

            if (HandleReceivedMetadata is null)
            {
                ErrorMessage("Metadata handler not defined.");
                return;
            }

            MeasurementMetadataMap metadata = new MeasurementMetadataMap();
            GetParsedMeasurementMetadata(metadata);
            HandleReceivedMetadata(metadata.ToDictionary(m => m.Key, m => m.Value.PointTag));
        }

        public override void SubscriptionUpdated(SignalIndexCache signalIndexCache)
        {
            StatusMessage($"Publisher provided {signalIndexCache.Count:N0} measurements in response to subscription.");
        }

        public override unsafe void ReceivedNewMeasurements(Measurement* measurements, int length)
        {
            const ulong interval = 10 * 60;
            ulong measurementCount = (ulong)length;
            bool showMessage = m_processCount + measurementCount >= (m_processCount / interval + 1) * interval;

            m_processCount += measurementCount;

            // Process received measurements
            for (int i = 0; i < length; i++)
                HandleReceivedMeasurement?.Invoke(measurements[i]);

            // Only display messages every few seconds
            if (showMessage)
                StatusMessage($"{GetTotalMeasurementsReceived():N0} measurements processed so far...");
        }

        protected override void ConfigurationChanged() =>
            StatusMessage("Configuration change detected. Metadata refresh requested.");

        protected override void HistoricalReadComplete() =>
            StatusMessage("Historical data read complete.");

        protected override void ConnectionEstablished() =>
            StatusMessage("Connection established.");

        protected override void ConnectionTerminated() =>
            StatusMessage("Connection terminated.");
    }
}