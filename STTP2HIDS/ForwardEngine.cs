//******************************************************************************************************
//  ForwardEngine.cs - Gbtc
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

using Gemstone;
using Gemstone.Collections.CollectionExtensions;
using HIDS;
using sttp;
using System;
using System.Collections.Generic;
using System.Text;

namespace STTP2HIDS
{
    public static partial class ForwardEngine
    {
        private static readonly Dictionary<Guid, PointStats> s_statistics;
        private static readonly object s_consoleLock;
        private static Settings s_settings = default!;
        private static API s_hidsAPI = default!;
        private static PointQueue s_pointQueue = default!;
        private static Dictionary<Guid, string>? s_metadata;
        private static ulong s_processedAggregations;
        private static long s_displayInterval;
        private static long s_lastDisplayTime;

        static ForwardEngine()
        {
            s_statistics = new Dictionary<Guid, PointStats>();
            s_consoleLock = new object();
        }

        public static ExitCode Start(Settings settings)
        {
            try
            {
                StringBuilder startMessage = new StringBuilder();
                Ticks startTime = DateTime.UtcNow.Ticks;

                s_settings = settings ?? throw new ArgumentNullException(nameof(settings));

                startMessage.AppendLine();
                startMessage.AppendLine($"Establishing forward to InfluxDB HIDS \"{s_settings.InfluxDBEndPoint}\" from STTP data received from \"{s_settings.STTPEndPoint}\":");
                startMessage.AppendLine();
                startMessage.AppendLine($"        Window Size: {s_settings.WindowSize:N0}ms");
                startMessage.AppendLine($"           Token ID: {s_settings.TokenID}");
                startMessage.AppendLine($"       Point Bucket: {s_settings.PointBucket}");
                startMessage.AppendLine($"    Organization ID: {s_settings.OrganizationID}");
                startMessage.AppendLine($"  Filter Expression: {s_settings.FilterExpression}");
                startMessage.AppendLine();
                startMessage.AppendLine($"Press any key to stop...");

                StatusMessage(startMessage.ToString());

                s_displayInterval = TimeSpan.FromMilliseconds(s_settings.WindowSize * 2).Ticks;

                using PointQueue pointQueue = CreatePointQueue();
                using API hidsAPI = ConnectHIDSClient();
                using SubscriberHandler subscriber = ConnectSTTPClient();
               
                Console.ReadKey();

                subscriber.Disconnect();
                hidsAPI.Disconnect();

                StatusMessage($"Total process runtime: {(DateTime.UtcNow.Ticks - startTime).ToElapsedTimeString(3)}");
                return ExitCode.Success;
            }
            catch (Exception ex)
            {
                ErrorMessage($"ERROR: {ex.Message}");
                return ExitCode.Exception;
            }
        }

        private static PointQueue CreatePointQueue()
        {
            s_pointQueue = new PointQueue();
            return s_pointQueue;
        }

        private static API ConnectHIDSClient()
        {
            ValidateEndPoint(s_settings.InfluxDBEndPoint, out string host, out ushort port);

            s_hidsAPI = new API
            {
                TokenID = s_settings.TokenID,
                PointBucket = s_settings.PointBucket,
                OrganizationID = s_settings.OrganizationID
            };

            s_hidsAPI.Connect($"http://{host}:{port}");

            return s_hidsAPI;
        }

        private static SubscriberHandler ConnectSTTPClient()
        {
            ValidateEndPoint(s_settings.STTPEndPoint, out string host, out ushort port);

            SubscriberHandler subscriberHandler = new SubscriberHandler
            {
                HandleStatusMessage = StatusMessage,
                HandleErrorMessage = ErrorMessage,
                HandleReceivedMetadata = HandleReceivedMetadata,
                HandleReceivedMeasurement = HandleReceivedMeasurement,
                FilterExpression = s_settings.FilterExpression
            };

            subscriberHandler.Initialize(host, port);
            subscriberHandler.ConnectAsync();

            return subscriberHandler;
        }

        private static void ValidateEndPoint(string? endPoint, out string host, out ushort port)
        {
            if (string.IsNullOrWhiteSpace(endPoint))
                throw new InvalidOperationException($"EndPoint is undefined");

            string[] parts = endPoint.Split(':', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            if (parts.Length != 2 || !ushort.TryParse(parts[1], out port))
                throw new InvalidOperationException($"EndPoint \"{endPoint}\" has an unrecognized format");

            host = parts[0];
        }

        private static void HandleReceivedMetadata(Dictionary<Guid, string> metadata) =>
            s_metadata = metadata;

        private static void HandleReceivedMeasurement(Measurement measurement)
        {
            Guid signalID = measurement.GetSignalID();
            PointStats stats = s_statistics.GetOrAdd(signalID, _ => new PointStats());

            if (stats.Update(measurement))
            {
                if (s_metadata is not null && s_metadata.TryGetValue(signalID, out string? pointTag) && !string.IsNullOrWhiteSpace(pointTag))
                {
                    s_pointQueue.Enqueue(stats, pointTag);
                    s_processedAggregations++;
                }

                stats.Reset();
            }

            if (s_processedAggregations > 1UL && DateTime.UtcNow.Ticks - s_lastDisplayTime > s_displayInterval)
            {
                s_lastDisplayTime = DateTime.UtcNow.Ticks;
                StatusMessage($"Processed {s_processedAggregations:N0} STTP measurement aggregations for publication to InfluxDB so far...");
            }
        }
        
        private static void StatusMessage(string message)
        {
            lock (s_consoleLock)
                Console.WriteLine($"{message}\n");
        }

        private static void ErrorMessage(string message)
        {
            lock (s_consoleLock)
                Console.Error.WriteLine($"{message}\n");
        }
    }
}