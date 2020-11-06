//******************************************************************************************************
//  ForwardEngine-PointQueue.cs - Gbtc
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

using Gemstone.Threading.SynchronizedOperations;
using Gemstone.Units;
using HIDS;
using System;
using System.Collections.Generic;
using System.Timers;

namespace STTP2HIDS
{
    partial class ForwardEngine
    {
        private sealed class PointQueue : IDisposable
        {
            private readonly ShortSynchronizedOperation m_publishOperation;
            private readonly List<Point> m_pointQueue;
            private readonly Timer m_publishTimer;
            private readonly string m_publicationInterval;
            private bool m_disposed;

            public PointQueue()
            {
                m_publishOperation = new ShortSynchronizedOperation(HandlePublication, HandleException);
                m_pointQueue = new List<Point>();
                m_publishTimer = new Timer(s_settings.WindowSize);
                m_publishTimer.Elapsed += PublishTimer_Elapsed;
                m_publicationInterval = Time.ToElapsedTimeString(TimeSpan.FromMilliseconds(m_publishTimer.Interval).TotalSeconds, 3).ToLower();
                m_publishTimer.Start();
            }

            private void PublishTimer_Elapsed(object sender, ElapsedEventArgs e)
            {
                if (m_publishOperation.IsRunning)
                    StatusMessage($"WARNING: Last InfluxDB publication operation still running after {m_publicationInterval}, publication may be falling behind. There are currently {m_pointQueue.Count:N0} points in the queue.");

                m_publishOperation.TryRun();
            }

            public void Enqueue(PointStats stats, string pointTag)
            {
                lock (m_pointQueue)
                {
                    m_pointQueue.Add(new Point
                    {
                        Tag = pointTag,
                        Minimum = stats.Minimum,
                        Maximum = stats.Maximum,
                        Average = stats.Total / stats.Count,
                        QualityFlags = (uint)stats.Flags,
                        Timestamp = stats.StartTimestamp
                    });
                }
            }

            public void Dispose()
            {
                if (m_disposed)
                    return;

                try
                {
                    if (m_publishTimer is not null)
                    {
                        m_publishTimer.Stop();
                        m_publishTimer.Elapsed -= PublishTimer_Elapsed;
                        m_publishTimer.Dispose();
                    }
                }
                finally
                {
                    m_disposed = true;
                }
            }

            private void HandlePublication()
            {
                Point[] points;

                lock (m_pointQueue)
                {
                    points = m_pointQueue.ToArray();
                    m_pointQueue.Clear();
                }

                if (points.Length == 0)
                    return;

                s_hidsAPI.WritePoints(points);
                StatusMessage($"Published {points.Length:N0} points to InfluxDB...");
            }

            private void HandleException(Exception ex) =>
                ErrorMessage($"ERROR: Exception publishing to InfluxDB: {ex.Message}");
        }
    }
}