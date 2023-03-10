using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using System.Text;
using MessageAzureQueue.Models;
using Azure.Messaging.ServiceBus;

namespace MessageAzureQueue.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductsController : ControllerBase
    {
        private readonly IConfiguration config;
        private readonly string connectionString;

        public ProductsController(IConfiguration config)
        {

            this.config = config;

            connectionString = this.config.GetValue<string>("AzureServiceBus");
        }

        // Método que é executado quando uma requisição HTTP POST é feita para a rota "/queue"
        [HttpPost("queue")]
        public async Task<IActionResult> PostQueue(Product product)
        {
    
            await SendMessageQueue(product);

            return Ok(product);
        }

        // Método privado que envia uma mensagem para a fila do Service Bus
        private async Task SendMessageQueue(Product product)
        {

            string queueName = "queuetest";

            // Cria uma nova instância do ServiceBusClient, passando a string de conexão com o Service Bus
            await using var client = new ServiceBusClient(connectionString);

            // Cria um novo sender para a fila especificada
            var sender = client.CreateSender(queueName);

            string messageBody = JsonSerializer.Serialize(product);

            // Cria uma nova mensagem do Service Bus, passando o corpo da mensagem serializado
            var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(messageBody));

            await sender.SendMessageAsync(message);
        }
    }

}
