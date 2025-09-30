using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Confluent.SchemaRegistry;
using Coelsa.Artifact.MessageBroker.Support.Handlers;
using Coelsa.Artifact.MessageBroker.Schemas;

namespace Coelsa.Artifact.MessageBroker;

public static class MessageBrokerExtension
{
    public static IServiceCollection AddMessageBroker(this IServiceCollection services, IConfiguration config)
    {
        // 1) Cargar settings desde "Kafka"
        var section = config.GetSection("Kafka");
        var options = section.Get<MessageBrokerSettings>()
            ?? throw new InvalidOperationException("Kafka configuration missing (section 'Kafka').");

        services.AddSingleton(options);

        // 2) (Opcional) Schema Registry cliente si hay URL
        if (!string.IsNullOrWhiteSpace(options.SchemaRegistryUrl))
        {
            services.AddSingleton<ISchemaRegistryClient>(_ =>
                new CachedSchemaRegistryClient(new SchemaRegistryConfig
                {
                    Url = options.SchemaRegistryUrl
                }));
        }

        // 3) Registrar CONCRETOS
        services.AddSingleton<KafkaPublisher>();
        services.AddSingleton<KafkaConsumer>();

        // 4) Mapear interfaces a los concretos (sin decorar acá)
        services.AddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<KafkaPublisher>());
        services.AddSingleton<IMessageConsumer>(sp => sp.GetRequiredService<KafkaConsumer>());

        return services;
    }
}
