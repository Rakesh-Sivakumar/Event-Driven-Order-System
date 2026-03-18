using Microsoft.EntityFrameworkCore;
using Order.API.Models;

namespace Order.API.Data
{
    public class OrderDbContext : DbContext
    {
        public OrderDbContext(DbContextOptions<OrderDbContext> options): base(options) { }

        public DbSet<OrderEntity> Orders { get; set; }

    }
}
