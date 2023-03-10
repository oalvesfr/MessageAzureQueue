using Azure.Messaging.ServiceBus;
using MessageAzureQueue.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MessageAzureQueue.HostedServices
{
    public class ProductQueueConsumer : IHostedService
    {
        private readonly ServiceBusClient _client;
        private readonly ServiceBusProcessor _processor;

        // Construtor que inicializa o cliente e o processador Service Bus com base nas informações de configuração
        public ProductQueueConsumer(IConfiguration config)
        {
            var serviceBusConnection = config.GetValue<string>("AzureServiceBus");

            _client = new ServiceBusClient(serviceBusConnection);

            // Cria uma instância do processador Service Bus para a fila "queuetest" com opções adicionais
            _processor = _client.CreateProcessor("queuetest", new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = 1,     // Número máximo de chamadas concorrentes que o processador fará para o manipulador de mensagens
                AutoCompleteMessages = false // Desativa a conclusão automática da mensagem após processamento
            });

            // Registra os métodos de retorno de chamada para processar mensagens e lidar com erros
            _processor.ProcessMessageAsync += ProcessMessagesAsync;
            _processor.ProcessErrorAsync += ExceptionReceivedHandler;
        }

        // Método StartAsync invocado quando o serviço é iniciado
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("############## Starting Consumer - Queue ####################");

            // Inicia o processamento de mensagens pelo processador
            await _processor.StartProcessingAsync(cancellationToken);
        }

        // Método StopAsync invocado quando o serviço é parado
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("############## Stopping Consumer - Queue ####################");

            // Para o processamento de mensagens pelo processador
            await _processor.StopProcessingAsync(cancellationToken);

            // Libera a conexão do cliente Service Bus
            await _client.DisposeAsync();
        }

        // Método privado invocado para processar mensagens recebidas pelo processador
        private async Task ProcessMessagesAsync(ProcessMessageEventArgs args)
        {
            Console.WriteLine("### Processing Message - Queue ###");
            Console.WriteLine($"{DateTime.Now}");
            Console.WriteLine($"Received message: SequenceNumber:{args.Message.SequenceNumber} Body:{Encoding.UTF8.GetString(args.Message.Body)}");

            Product _product = JsonSerializer.Deserialize<Product>(args.Message.Body);

            // Completa a mensagem após processá-la com êxito
            await args.CompleteMessageAsync(args.Message);
        }

        // Método privado invocado para lidar com erros ocorridos durante o processamento de mensagens
        private Task ExceptionReceivedHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine($"Message handler encountered an exception {args.Exception}.");
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Identifier: {args.Identifier}");
            Console.WriteLine($"- Entity Path: {args.EntityPath}");
            Console.WriteLine($"- CancellationToken: {args.CancellationToken}");

            return Task.CompletedTask;
        }
    }

}
