# Fast doubling algorithm for Fibonacci numbers
# F(2k) = F(k)(2F(k+1) - F(k))
# F(2k+1) = F(k+1)^2 + F(k)^2
# Source: https://km.mmf.bsu.by/courses/2021/vp2/fibonacci.pdf

def fib_fast_doubling(n: int) -> int:
    """
    Calculate nth Fibonacci number using fast doubling algorithm.
    
    Time complexity: O(log n)
    Space complexity: O(log n) due to recursion
    
    Args:
        n: Non-negative integer index
        
    Returns:
        nth Fibonacci number
        
    Raises:
        ValueError: If n is negative
    """
    if n < 0:
        raise ValueError("N must be non-negative")

    def _fd(k: int) -> tuple[int, int]:
        if k == 0:
            return 0, 1
        a, b = _fd(k >> 1)
        c = a * (2 * b - a)
        d = a * a + b * b
        if k & 1:
            return d, c + d
        return c, d

    return _fd(n)[0]
