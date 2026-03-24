using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Order.Domain.Entities;
using Order.Infrastructure.Caching;
using Order.Infrastructure.Data;
using Order.Infrastructure.Messaging;
using Shared.Contracts;

namespace Order.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrdersController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly OrderDbContext _context;
        private readonly RedisService _redisService;
        private readonly ILogger<OrdersController> _logger;


        public OrdersController(OrderDbContext dbContext, RedisService redisService, ILogger<OrdersController> logger, IConfiguration configuration)
        {
            _context = dbContext;
            _redisService = redisService;
            _logger = logger;
            _configuration = configuration;
        }

        [HttpPost]
        public async Task<IActionResult> CreateOrder(OrderEntity order, [FromServices] KafkaProducer producer)
        {
            var cacheKey = $"invetory:{order.ProductName}";

            int stock;

            //Check redis first

            var cachedStock = await _redisService.GetAsync(cacheKey);

            if (!string.IsNullOrEmpty(cachedStock))
            {
                _logger.LogInformation("Cache hit (order api)");
                stock = int.Parse(cachedStock);
            }
            else
            {
                _logger.LogInformation("Cache miss --> Db hit");

                using var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection"));
                await connection.OpenAsync();

                var cmd = new SqlCommand(
                                        "SELECT Stock FROM Inventory WHERE ProductName = @Name",
                                        connection);

                cmd.Parameters.AddWithValue("@Name", order.ProductName);

                var result = await cmd.ExecuteScalarAsync();

                if (result == null)
                    return BadRequest("Product not found");

                stock = (int)result;

                // Save to Redis
                await _redisService.SetAsync(cacheKey, stock.ToString());
            }

            //Validate stock

            if (stock < order.Quantity)
            {
                return BadRequest("Out of stock");
            }

            // create order 

            order.Status = "Created";

            _context.Orders.Add(order);
            await _context.SaveChangesAsync();

            var eventMessage = new OrderCreatedEvent
            {
                OrderId = order.Id,
                ProductName = order.ProductName,
                Quantity = order.Quantity
            };

            await producer.ProducerAsync("orders", eventMessage);

            return Ok(order);
        }

        [HttpGet("{id}")]
        public async Task<IActionResult> GetOrder(int id)
        {
            var order = await _context.Orders.FindAsync(id);

            if (order == null)
                return NotFound();

            return Ok(order);
        }

    }
}
