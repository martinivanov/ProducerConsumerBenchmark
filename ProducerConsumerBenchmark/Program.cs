using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace ProducerConsumerBetnchmark
{
    public abstract class ProducerConsumerProcessor
    {
        private readonly int _workers;

        protected ProducerConsumerProcessor(int workerCount)
        {
            _workers = workerCount;
        }

        public abstract void EnqueueWork(Action work);

        public abstract void Complete();
    }
    
    public class PlainOldQueueProcessor : ProducerConsumerProcessor
    {
        private readonly List<Task> _tasks;
        private readonly Queue<Action> _workQueue = new Queue<Action>();
        private readonly object _queueLock = new object();
        
        private bool _work = true;

        public PlainOldQueueProcessor(int workerCount) : base(workerCount)
        {
            _tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() => DoWork())).ToList();
        }

        public override void EnqueueWork(Action work)
        {
            lock (_queueLock)
            {
                _workQueue.Enqueue(work);                
            }
        }

        public override void Complete()
        {
            _work = false;
        }

        private void DoWork()
        {
            while (true)
            {
                lock (_queueLock)
                {
                    if (!_work && _workQueue.Count == 0)
                    {
                        break;
                    }
                    
                    if (_workQueue.TryDequeue(out var work))
                    {
                        work();
                    }
                }
            }
        }
    }

    public class BlockingCollectionProcessor : ProducerConsumerProcessor
    {
        private readonly List<Task> _tasks;
        private readonly BlockingCollection<Action> _workQueue = new BlockingCollection<Action>();
        
        public BlockingCollectionProcessor(int workerCount) : base(workerCount)
        {
            _tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() => DoWork())).ToList();
        }

        public override void EnqueueWork(Action work)
        {
            _workQueue.Add(work);
        }

        public override void Complete()
        {
            _workQueue.CompleteAdding();
        }
        
        private void DoWork()
        {
            foreach (var work in _workQueue.GetConsumingEnumerable())
            {
                work();
            }
        }
    }
    
    public class ChannelProcessor : ProducerConsumerProcessor
    {
        private readonly List<Task> _tasks;
        private readonly Channel<Action> _workQueue = Channel.CreateUnbounded<Action>(new UnboundedChannelOptions
        {
            SingleWriter = true
        });
        

        
        public ChannelProcessor(int workerCount) : base(workerCount)
        {
            _tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() => DoWork())).ToList();
        }

        public override void EnqueueWork(Action work)
        {
            _workQueue.Writer.TryWrite(work);
        }

        public override void Complete()
        {
            _workQueue.Writer.Complete();
        }
        
        private async Task DoWork()
        {
            while (await _workQueue.Reader.WaitToReadAsync())
            {
                while (_workQueue.Reader.TryRead(out var work))
                {
                    work();
                }
            }
        }
    }

    public class ProducerConsumerStructuresBenchmark
    {
        private const int N = 1000000;
        private const int WorkerCount = 4;

        [Benchmark]
        public void PlainOldQueue() => Run(new PlainOldQueueProcessor(WorkerCount));

        [Benchmark]
        public void BlockingCollection() => Run(new BlockingCollectionProcessor(WorkerCount));
        
        [Benchmark]
        public void ChannelProcessor() => Run(new ChannelProcessor(WorkerCount));
        
        private void Run(ProducerConsumerProcessor processor)
        {
            var waiter = new AutoResetEvent(false);
            
            for (int i = 0; i < N; i++)
            {
                processor.EnqueueWork(() => { /* slack */});
            }
            
            processor.EnqueueWork(() => waiter.Set());
            
            processor.Complete();

            waiter.WaitOne();
        }
    }
    
    class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<ProducerConsumerStructuresBenchmark>();

            Console.WriteLine("Done");
        }
    }
}