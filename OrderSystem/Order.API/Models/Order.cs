namespace Order.API.Models
{
    public class OrderEntity
    {
        public int Id { get; set; }
        public string ProductName { get; set; }
        public int Quantity { get; set; }
        public string Status { get; set; }
    }
}
