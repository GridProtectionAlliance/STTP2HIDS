//******************************************************************************************************
//  ForwardEngine-PointStats.cs - Gbtc
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

namespace STTP2HIDS
{
    partial class ForwardEngine
    {
        private sealed class PointStats
        {
            public DateTime StartTimestamp;
            public DateTime LastTimestamp;
            public double Minimum;
            public double Maximum;
            public double Total;
            public int Count;
            public MeasurementStateFlags Flags;

            public PointStats() => Reset();

            public bool Update(Measurement measurement)
            {
                double value = measurement.Value;
                DateTime timestamp = measurement.GetDateTime();

                if (StartTimestamp == default)
                    StartTimestamp = timestamp;

                // Check for stale point
                if (LastTimestamp != default && (timestamp - LastTimestamp).TotalMilliseconds > s_settings.WindowSize)
                {
                    Reset();
                    return false;
                }

                LastTimestamp = timestamp;

                if (value < Minimum)
                    Minimum = value;

                if (value > Maximum)
                    Maximum = value;

                Total += value;
                Count++;
                Flags |= measurement.Flags;

                return (timestamp - StartTimestamp).TotalMilliseconds > s_settings.WindowSize;
            }

            public void Reset()
            {
                StartTimestamp = default;
                LastTimestamp = default;
                Minimum = double.MaxValue;
                Maximum = double.MinValue;
                Total = default;
                Count = default;
                Flags = MeasurementStateFlags.Normal;
            }
        }
    }
}