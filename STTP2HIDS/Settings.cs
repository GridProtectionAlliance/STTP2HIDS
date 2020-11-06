//******************************************************************************************************
//  Settings.cs - Gbtc
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Gemstone.Configuration.AppSettings;
using Microsoft.Extensions.Configuration;
using static STTP2HIDS.ExitCode;

namespace STTP2HIDS
{
    public enum ExitCode
    {
        Success = 0,
        InvalidCommandLineArgs = 1,
        InvalidSTTPEndPoint = 2,
        InvalidInfluxDBEndPoint = 3,
        Exception = 254,
        HelpDisplay = 255
    }

    public class Settings
    {
        public const string InfluxDBSection = "InfluxDB";
        public const string STTPSection = "STTP";

        // Root application default settings
        public const int DefaultWindowSize = 5000;

        // InfluxDB default settings
        public const string DefaultTokenIP = "";
        public static string DefaultPointBucket = "point_bucket";
        public static string DefaultOrganizationID = "gpa";

        // STTP default settings
        public const string DefaultFilterExpression = "FILTER ActiveMeasurements WHERE SignalType LIKE '%PHM' OR SignalType = 'FREQ'";

        // Fixed postion settings from command line
        public string? STTPEndPoint;
        public string? InfluxDBEndPoint;

        // Optional settings (defaults from config file)
        public int WindowSize { get; set; }
        public string TokenID { get; set; }
        public string PointBucket { get; set; }
        public string OrganizationID { get; set; }
        public string FilterExpression { get; set; }

        public Settings(IConfiguration configuration)
        {
            WindowSize = int.Parse(configuration[nameof(WindowSize)]);

            IConfigurationSection influxDBSettings = configuration.GetSection(InfluxDBSection);
            TokenID = influxDBSettings[nameof(TokenID)];
            PointBucket = influxDBSettings[nameof(PointBucket)];
            OrganizationID = influxDBSettings[nameof(OrganizationID)];

            IConfigurationSection sttpSettings = configuration.GetSection(STTPSection);
            FilterExpression = sttpSettings[nameof(FilterExpression)];
        }

        public static void ConfigureAppSettings(IAppSettingsBuilder builder)
        {
            // Root configuration settings
            builder.Add($"{nameof(WindowSize)}", DefaultWindowSize.ToString(), "Defines the window size, in milliseconds, overwhich to calculate min, max and avg statistics for HIDS InfluxDB target.");

            // InfluxDB configuration settings
            builder.Add($"{InfluxDBSection}:{nameof(TokenID)}", DefaultTokenIP, "Defines the InfluxDB token ID needed for server authentication.");
            builder.Add($"{InfluxDBSection}:{nameof(PointBucket)}", DefaultPointBucket, "Defines the InfluxDB point bucket.");
            builder.Add($"{InfluxDBSection}:{nameof(OrganizationID)}", DefaultOrganizationID, "Defines the InfluxDB organization ID.");

            // STTP configuration settings
            builder.Add($"{STTPSection}:{nameof(FilterExpression)}", DefaultFilterExpression, "Defines the STTP filter expression for measurement subscription.");
        }

        public static Dictionary<string, string> SwitchMappings => new Dictionary<string, string>
        {
            [$"--{nameof(TokenID)}"] = $"{InfluxDBSection}:{nameof(TokenID)}",
            [$"--{nameof(PointBucket)}"] = $"{InfluxDBSection}:{nameof(PointBucket)}",
            [$"--{nameof(OrganizationID)}"] = $"{InfluxDBSection}:{nameof(OrganizationID)}",
            [$"--{nameof(FilterExpression)}"] = $"{STTPSection}:{nameof(FilterExpression)}",
            ["-w"] = nameof(WindowSize),
            ["-t"] = $"{InfluxDBSection}:{nameof(TokenID)}",
            ["-p"] = $"{InfluxDBSection}:{nameof(PointBucket)}",
            ["-o"] = $"{InfluxDBSection}:{nameof(OrganizationID)}",
            ["-f"] = $"{STTPSection}:{nameof(FilterExpression)}",
        };

        public ExitCode Parse(string[] args)
        {
            HashSet<string> optionArgs = args.Where(arg => arg.StartsWith('-') || (arg.StartsWith('/'))).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (optionArgs.Contains("--help") || optionArgs.Contains("-?") || optionArgs.Contains("/?"))
            {
                ShowHelp();
                return HelpDisplay;
            }

            args = args.Where(arg => !optionArgs.Contains(arg)).ToArray();

            if (args.Length != 2)
            {
                HandleError($"Expected 2 fixed arguments, received {args.Length:N0}.");
                return InvalidCommandLineArgs;
            }

            if (!TryParseEndPoint(args[0], out STTPEndPoint))
            {
                HandleError($"Bad STTP end point \"{args[0]}\".");
                return InvalidSTTPEndPoint;
            }

            if (!TryParseEndPoint(args[1], out InfluxDBEndPoint))
            {
                HandleError($"Bad InfluxDB end point \"{args[1]}\".");
                return InvalidInfluxDBEndPoint;
            }

            return Success;
        }

        private static bool TryParseEndPoint(string source, out string? endPoint)
        {
            if (IPEndPoint.TryParse(source, out IPEndPoint? result))
            {
                endPoint = result.ToString();
                return true;
            }

            string[] parts = source.Split(':', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            if (parts.Length == 2 && ushort.TryParse(parts[1], out ushort port))
            {
                endPoint = $"{parts[0]}:{port}";
                return true;
            }

            endPoint = default;
            return false;
        }

        private static void HandleError(string errorMessage)
        {
            Console.Error.WriteLine($"ERROR: {errorMessage}{Environment.NewLine}");
            ShowHelp();
        }

        private static void ShowHelp()
        {
            Console.WriteLine("USAGE:");
            Console.WriteLine($"    {nameof(STTP2HIDS)} [options] sttpHost:sttpPort influxHost:influxPort");
            Console.WriteLine();
            Console.WriteLine("OPTIONS:");
            Console.WriteLine($"  -w, --{nameof(WindowSize)}        Defines the window size, in milliseconds, overwhich to calculate statistics");
            Console.WriteLine($"  -t, --{nameof(TokenID)}           Defines the InfluxDB token ID needed for server authentication");
            Console.WriteLine($"  -p, --{nameof(PointBucket)}       Defines the InfluxDB point bucket");
            Console.WriteLine($"  -o, --{nameof(OrganizationID)}    Defines the InfluxDB organization ID");
            Console.WriteLine($"  -f, --{nameof(FilterExpression)}  Defines the STTP filter expression for measurement subscription");
            Console.WriteLine("  -?, --help              Shows usage");
            Console.WriteLine();
            Console.WriteLine("EXAMPLES:");
            Console.WriteLine("  > Forward STTP from 10.21.35.90:7165 to InfluxDB on 10.21.35.95:8086:");
            Console.WriteLine($"       {nameof(STTP2HIDS)} 10.21.35.90:7165 10.21.35.95:8086{Environment.NewLine}");
            Console.WriteLine("  > Forward STTP to InfluxDB with a specific token ID:");
            Console.WriteLine($"       {nameof(STTP2HIDS)} -t=Qv02== openhistorian:7175 influxdb:8086{Environment.NewLine}");
        }
    }
}