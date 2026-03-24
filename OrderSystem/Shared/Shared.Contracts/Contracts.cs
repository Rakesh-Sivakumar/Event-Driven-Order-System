namespace Shared.Contracts
{
    public class OrderCreatedEvent
    {
        public int OrderId { get; set; }
        public string? ProductName { get; set; }
        public int Quantity { get; set; }
    }
    public class PaymentCompletedEvent
    {
        public string? EventId { get; set; }
        public int OrderId { get; set; }
        public string? ProductName { get; set; }
        public int Quantity { get; set; }
        public string? Status { get; set; }
    }
}
