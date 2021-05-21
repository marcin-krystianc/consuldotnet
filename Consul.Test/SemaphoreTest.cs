// -----------------------------------------------------------------------
//  <copyright file="SemaphoreTest.cs" company="PlayFab Inc">
//    Copyright 2015 PlayFab Inc.
//    Copyright 2020 G-Research Limited
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Consul.Test
{
    // These tests are slow, so we put them into separate collection so they can run in parallel to other tests.
    [Trait("speed", "slow")]
    [Collection("SemaphoreTest")]
    public class SemaphoreTest : IDisposable
    {
        private ConsulClient _client;
        const int DefaultSessionTTLSeconds = 10;
        const int LockWaitTimeSeconds = 15;

        public SemaphoreTest()
        {
            _client = new ConsulClient(c =>
            {
                c.Token = TestHelper.MasterToken;
                c.Address = TestHelper.HttpUri;
            });
        }

        public void Dispose()
        {
            _client.Dispose();
        }

        [Fact]
        public async Task Semaphore_OneShot()
        {
            const string keyName = "test/semaphore/oneshot";
            var semaphoreOptions = new SemaphoreOptions(keyName, 2) {SemaphoreTryOnce = true};

            semaphoreOptions.SemaphoreWaitTime = TimeSpan.FromMilliseconds(1000);

            var semaphoreKey = _client.Semaphore(semaphoreOptions);

            await semaphoreKey.Acquire(CancellationToken.None);
            Assert.True(semaphoreKey.IsHeld);

            var another = _client.Semaphore(new SemaphoreOptions(keyName, 2)
            {
                SemaphoreTryOnce = true, SemaphoreWaitTime = TimeSpan.FromMilliseconds(1000)
            });

            await another.Acquire();
            Assert.True(another.IsHeld);
            Assert.True(semaphoreKey.IsHeld);

            var contender = _client.Semaphore(new SemaphoreOptions(keyName, 2)
            {
                SemaphoreTryOnce = true, SemaphoreWaitTime = TimeSpan.FromMilliseconds(1000)
            });

            var stopwatch = Stopwatch.StartNew();

            await TimeoutUtils.WithTimeout(
                Assert.ThrowsAsync<SemaphoreMaxAttemptsReachedException>(async () => await contender.Acquire()));

            Assert.False(contender.IsHeld, "Contender should have failed to acquire");
            Assert.False(stopwatch.ElapsedMilliseconds < semaphoreOptions.SemaphoreWaitTime.TotalMilliseconds);

            Assert.False(contender.IsHeld);
            Assert.True(another.IsHeld);
            Assert.True(semaphoreKey.IsHeld);
            await semaphoreKey.Release();
            await another.Release();
            await contender.Destroy();
        }
    }
}
