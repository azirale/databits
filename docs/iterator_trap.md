# Traps In Reusing Python Iterators

It is somewhat common when processing data in Python, to iterate over a set of values.
However while Iterators, Iterables, and Generators can all be used in essentially the same way in Python, they do *not* work the same.

## Iterators, Iterables, *and* Generators?

A generator in Python is an object that returns multiple values one-at-a-time through a call to its `__next__()` method.
It can be implemented through a class, a function using `yield`, or [even through an inline expression](https://docs.python.org/3/reference/expressions.html#generator-expressions).

This makes using a `Generator` very similar to using an `Iterator` or an `Iterable`, however there are some important differences.

An `Iterator` is a specific one-off state-tracking object that is used to advance position through some `Iterable` by calling the Iterator's `__next__()` method.
Iterators are *not* reusable, once a call is made to its `__next__()` method it is not possible to go back over the same value.
You must create a *new* Iterator over the original Iterable.

An `Iterable` is an object with an `__iter__()` method, which returns the appropriate `Iterator` for that object.
This is the only behaviour an Iterable guarantees -- that you can get its Iterator.
There is no guarantee that you will get either the *same* Iterator on each call, or if you will get a *new* Iterator on each call.

A `Generator` is an `Iterable` that does not have all of its data in memory, and instead 'generates' it as it goes.
An example for a generator could be going through the Fibonacci numbers -- it is an infinite sequence and there's no way to hold them all in memory, and every time you get the next value you want to advance its state so the following call gets the number after that.

The fact that a generator cannot hold its data in memory means that its Iterator is ***not*** reusable.
A given generator has its own internal state to generate new values, which can be useful as it allows you to exit a loop on its Iterator, then pick it up again later.
The trick then is that if you need to iterate over the same sequence, you must create a new instance of the generator.

This is in contrast to fully in-memory Iterable objects that will automatically create a new Iterator on each call, because the Iterable has no internal state.
Lists, tuples, and range objects all work this way -- every time you iterate over them you spawn a *new* iterator.

## Example Code

```py
from typing import Iterable, Iterator, Generator

# the `range` class is an Iterable but not a Generator - it has static values
range_as_iterable:Iterable = range(7)

# this is a generator expression, it can be iterated over
generator_expression:Generator = (x for x in range_as_iterable)
# type checking will show that this is valid - a generator is an iterable
generator_as_iterable:Iterable = generator_expression


# you can directly get the iterator for the range
range_iter1:Iterator = range_as_iterable.__iter__()
range_iter2:Iterator = range_as_iterable.__iter__()
# these objects are different - we got a different iterator each time
range_iter1 == range_iter2
#> False


# you can also directly get the iterator for a generator
gen_iter_1:Iterator = generator_expression.__iter__()
gen_iter_2:Iterator = generator_expression.__iter__()
# this time the iterators ARE the same object
gen_iter_1 == gen_iter_2
#> True


# iterators are NOT reusable - repeated uses will be empty/ended
len([*range_iter1])
#> 7
len([*range_iter1])
#> 0

# the second iterator has a separate state, so still works once
len([*range_iter2])
#> 7
# but not twice
len([*range_iter2])

# we saw earlier that generators give the same iterator
# therefore a generator can only be iterated once
len([*gen_iter_1])
#> 7
# even if we made a second reference to its iterator
len([*gen_iter_2])
#> 0
```

## The Trap Springs

If you have a function that returns an Iterable it does not give any direct type information to let you know if it is a static iterable whose `__iter__()` call creates new Iterator objects, and thus can be iterated multiple times, or if you have a Generator that returns the *same* Iterable each time.

Let's look at an example that, at first glance, looks like generators are reusable.

```py
# the `yield` keyword means this function returns a Generator
def fetch_values()->Iterable[int]:
    for i in range(10):
        yield i

# this gets the expected 10 values in range(10)
len([*fetch_values()])

# a second call to the generator *also* gets 10 values
len([*fetch_values()])
```

This is because each call to `fetch_values()` creates a *new* instance of a generator, it does not give you the *same* generator.
Since each generator has its own iterator, then each instance of the generator can be iterated over independently.

That breaks down if you try to re-use a generator.
Here, for example, we reuse the reference rather than calling the generator function again, and our second attempt at iterating fails.

```py
my_generator = fetch_values()
len([*my_generator])
#> 10
len([*my_generator])
#> 0
```

That behaviour can be useful if you want to stop and start evaluating the generator in some other loop, particular if you don't want to just nest calls.
You can pass an existing generator instance into a processor, and it will pick up where previous processing left off.

```py
def faux_pull_from_stream()->Iterable[int]:
    # let's pretend this generator is pulling values from some query
    fake_number_of_values_in_query = 100
    for _ in range(fake_number_of_values_in_query):
        yield 17


def get_next_chunk(value_pull:Iterable[int],chunk_limit:int)->list[int]:
    chunk:list[int] = []
    for value in value_pull:
        chunk.append(value)
        if len(chunk)>=chunk_limit:
            return chunk
    return chunk

value_pull = faux_pull_from_stream()
len(get_next_chunk(value_pull,33))
#> 33
len(get_next_chunk(value_pull,33))
#> 33
len(get_next_chunk(value_pull,33))
#> 33
len(get_next_chunk(value_pull,33))
#> 1 -- because there was only 1 value left in the generator out of 100
```

The real 'gotcha' that can appear is if you try to improve the performance of a generator - particularly sequence generators like for Fibonacci numbers - by adding a cache.

A cached or memoised function that returns a generator will return the *same* generator for given parameters.
This means you will be *reusing* the existing generator, and will be picking up where it left off.
You *will not* be starting for the start but reusing prior generated values.

```py
from functools import cache

@cache
def first_hundred_fib_gen()->Iterable[int]:
    a = 0
    b = 1
    for _ in range(100):
        yield a
        a,b = b,a+b

len([*first_hundred_fib_gen()])
#> 100
len([*first_hundred_fib_gen()])
#> 0 -- oops, we got the cached generator instance
```

So don't cache generators, or you'll suddenly get wildly different behaviour.