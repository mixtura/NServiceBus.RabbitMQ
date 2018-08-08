namespace NServiceBus.Transports.RabbitMQ
{
    using Routing;
    using System.Collections.Generic;
    using RabbitMQHeaders = global::RabbitMQ.Client.Headers;

    class RabbitMqQueueCreator : ICreateQueues
    {
        public IManageRabbitMqConnections ConnectionManager { get; set; }

        public IRoutingTopology RoutingTopology { get; set; }

        public Configure Configure { get; set; }

        public void CreateQueueIfNecessary(Address address, string account)
        {
            using (var connection = ConnectionManager.GetAdministrationConnection())
            using (var channel = connection.CreateModel())
            {
                var arguments = new Dictionary<string, object>();

                if (Configure.Settings.HasSetting(RabbitMQHeaders.XMaxPriority) && address.Queue.StartsWith(Configure.Settings.EndpointName()))
                {
                    // we cast priority to int as RabbitMQ client can't parse byte value
                    arguments.Add(RabbitMQHeaders.XMaxPriority, (int)Configure.Settings.Get<byte>(RabbitMQHeaders.XMaxPriority));
                }

                channel.QueueDeclare(address.Queue, Configure.DurableMessagesEnabled(), false, false, arguments);

                RoutingTopology.Initialize(channel, address.Queue);
            }
        }
    }
}