
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Confluent.Kafka.StreamBenchmark;

namespace Confluent.Kafka.Benchmark
{
    public class Program
    {

        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<StreamVsOld>(ManualConfig
                .Create(DefaultConfig.Instance)
                .WithOptions(ConfigOptions.KeepBenchmarkFiles));

            //BenchmarkRunner.Run<StreamVsOld>(new DebugInProcessConfig());
        }
    }
}
