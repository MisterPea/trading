class StrategyManager:
    def __init__(self, schwab_streaming):
        self.schwab_streaming = schwab_streaming
        self.strategies = {}

    def register_strategy(self, strategy_name, strategy):
        # Register a trading strategy with the manager
        self.strategies[strategy_name] = strategy
        # Set up the necessary data subscriptions and callbacks
        self.schwab_streaming.subscribe('quotes', strategy.on_quote)
        self.schwab_streaming.subscribe('orders', strategy.on_order_update)

    def unregister_strategy(self, strategy_name):
        # Unregister a trading strategy from the manager
        strategy = self.strategies.pop(strategy_name)
        self.schwab_streaming.unsubscribe('quotes', strategy.on_quote)
        self.schwab_streaming.unsubscribe('orders', strategy.on_order_update)

    @staticmethod
    def run():
        # Start the strategy execution loop
        while True:
            pass
            # Process data updates and execute the strategies
            # ...
