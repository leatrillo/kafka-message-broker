using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Confluent.SchemaRegistry;
using Coelsa.Artifact.MessageBroker.Support.Handlers;
using Coelsa.Artifact.MessageBroker.Schemas;

namespace Coelsa.Artifact.MessageBroker;

public static class MessageBrokerExtension
{
    public static IServiceCollection AddMessageBroker(this IServiceCollection services, IConfiguration config, string sectionName = "Kafka")
    {
        var options = config.GetSection(sectionName).Get<MessageBrokerSettings>()
            ?? throw new InvalidOperationException($"Configuration section '{sectionName}' is missing or invalid.");

        services.AddSingleton(options);

        if (!string.IsNullOrWhiteSpace(options.SchemaRegistryUrl))
        {
            services.AddSingleton<ISchemaRegistryClient>(sp => new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                Url = options.SchemaRegistryUrl!
            }));

            if (options.Avro is not null)
            {
                services.AddSingleton<IAvroSchemaResolver>(sp => new FileAvroSchemaResolver(options));
            }
        }

        services.AddSingleton<IMessagePublisher, KafkaPublisher>();
        services.AddSingleton<IMessageConsumer, KafkaConsumer>();
        return services;
    }
}
