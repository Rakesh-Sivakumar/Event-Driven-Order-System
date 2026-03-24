namespace Order.Domain.Entities
{
    public class OrderEntity
    {
        public int Id { get; set; }
        public string? ProductName { get; set; }
        public int Quantity { get; set; }
        public string? Status { get; set; }
    }
}
