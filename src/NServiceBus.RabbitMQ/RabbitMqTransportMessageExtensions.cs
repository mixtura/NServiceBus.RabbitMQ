﻿namespace NServiceBus.Transports.RabbitMQ
{
    using global::RabbitMQ.Client;
    using System;
    using System.Linq;
    using Unicast;
    using RabbitMQHeaders = global::RabbitMQ.Client.Headers;

    static class RabbitMqTransportMessageExtensions
    {
        public static void FillRabbitMqProperties(TransportMessage message, DeliveryOptions options, IBasicProperties properties)
        {
            properties.MessageId = message.Id;

            if (!String.IsNullOrEmpty(message.CorrelationId))
            {
                properties.CorrelationId = message.CorrelationId;
            }

            if (message.TimeToBeReceived < TimeSpan.MaxValue)
            {
                properties.Expiration = message.TimeToBeReceived.TotalMilliseconds.ToString();
            }

            properties.Persistent = message.Recoverable;

            properties.Headers = message.Headers.ToDictionary(p => p.Key, p => (object)p.Value);

            if (message.Headers.ContainsKey(NServiceBus.Headers.EnclosedMessageTypes))
            {
                properties.Type = message.Headers[NServiceBus.Headers.EnclosedMessageTypes].Split(new[]
                {
                    ','
                }, StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
            }

            if (message.Headers.ContainsKey(NServiceBus.Headers.ContentType))
            {
                properties.ContentType = message.Headers[NServiceBus.Headers.ContentType];
            }
            else
            {
                properties.ContentType = "application/octet-stream";
            }

            byte priority;
            if (message.Headers.ContainsKey(RabbitMQHeaders.XPriority) && byte.TryParse(message.Headers[RabbitMQHeaders.XPriority], out priority))
            {
                properties.Priority = priority;
            }

            var replyToAddress = options.ReplyToAddress ?? message.ReplyToAddress;
            if (replyToAddress != null)
            {
                properties.ReplyTo = replyToAddress.Queue;
            }
        }
    }
}