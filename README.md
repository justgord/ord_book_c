
## ord_book_c : a simple 'old-school' buy-sell order book in C code

avoids ram allocations by using preallocated chunk lists

reasonably fast : Buy-Sell match takes on average 1.1us / 1100ns
    
shows some higher latency outliers due to linux polling each core at 4KHz ?!?

main idea is to just jump to the current price level where buys and sells converge and scan for matches there.

type 'make' to build and run simple test with random buy/sells and some latency performance stats

TODO : read orders from standard FIX xml or ITCH binary data formats
