# LEC 5: Go, Threads, and Raft

# Read

[The Go Memory Model](https://golang.org/ref/mem)



## Question

> Consider the following code from the "incorrect synchronization" examples:
>
> ```go
> var a string
> var done bool
> 
> func setup() {
> 	a = "hello, world"
> 	done = true
> }
> 
> func main() {
> 	go setup()
> 	for !done {
> 	}
> 	print(a)
> }
>   
> ```
>
> Using the synchronization mechanisms of your choice, fix this code so it is guaranteed to have the intended behavior according to the Go language specification. Explain why your modification works in terms of the happens-before relation.



## Answer

```go
var a string
var done bool
var newDone chan bool

func setup() {
	a = "hello, world"
	newDone <- true
}

func main() {
	newDone = make(chan bool)
	go setup()
	<- newDone
	print(a)
}
```

Use an unbuffered  `channel` `newDown` to guarantee that the program will print *hello world* which means `setup()` happens before `print(a)`.

When the code goes to `<- newDone` in `main()`, it will wait until received a value from channel `newDown`. 

And the value will be put in `newDown` in `setup()`.