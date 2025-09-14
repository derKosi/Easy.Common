namespace Easy.Common.Extensions;

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;

/// <summary>
/// Extension methods for <see cref="HttpClient"/>.
/// </summary>
public static class HttpClientExtensions
{
    /// <summary>
    /// Consumes a streaming endpoint which returns a sequence entries of type <typeparamref name="T"/>.
    /// </summary>
    public static async IAsyncEnumerable<T> Consume<T>(this HttpClient http,
        Uri endpoint, JsonSerializerOptions? jsonOptions = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            Stream? stream = null;
            IAsyncEnumerator<T?>? enumerator = null;
            try
            {
                stream = await http.GetStreamAsync(endpoint, cancellationToken).ConfigureAwait(false);
                enumerator = EasyJson.DeserializeAsyncEnumerable<T>(stream, jsonOptions, cancellationToken).GetAsyncEnumerator(cancellationToken);
            }
            catch (HttpRequestException)
            {
                // failed to obtain stream - retry outer loop
                if (stream is not null)
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }

                if (enumerator is not null)
                {
                    await enumerator.DisposeAsync().ConfigureAwait(false);
                }

                continue;
            }

            bool errorOccurred = false;
            try
            {
                // read the stream until end or exception
                while (!cancellationToken.IsCancellationRequested)
                {
                    bool hasNext;
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (IOException)
                    {
                        errorOccurred = true;
                        break;
                    }

                    if (!hasNext)
                    {
                        // reached end-of-stream normally
                        break;
                    }

                    if (enumerator.Current is { } item)
                    {
                        yield return item;
                    }
                }
            }
            finally
            {
                await stream.DisposeAsync().ConfigureAwait(false);
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

            if (errorOccurred)
            {
                continue;
            }

            // we have either hit end-of-stream or cancellation, therefore we exit
            yield break;
        }
    }
}