Link: https://stackoverflow.com/questions/71893769/apache-beam-python-returning-conditional-statement-using-pardo-class?noredirect=1#comment127065634_71893769

I have written a ParDo FilterFn that Tags output based of isGoodRow function. You can rewrite the logic with your requirements ex. null checks, data validation etc. The other option could be to use a Partition Function for this use case.

The sample.txt I used for this example:

1,Foo,FooBarFooBarFooBar,1000
2,Foo,Bar,10
3,Foo,FooBarFooBarFooBar,900
4,Foo,FooBar,800
Output of good.txt

1,Foo,FooBarFooBarFooBar,1000
3,Foo,FooBarFooBarFooBar,900
Output of bad.txt

2,Foo,Bar,10
4,Foo,FooBar,800
