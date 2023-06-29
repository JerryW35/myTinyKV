# TinyKV project1

#### part1 StandAloneStorage

根据project1的描述，StandAloneStorage应该作为单机存储引擎。

文件中提供了NewStandAloneStorage、Start、Stop、Reader、Write方法需要完成.

我们应该根据util中提供的方法来补全这些内容。

在util的engines.go中的方法可以辅助我们完成这一部分。

首先，我们定义一下StandAloneStorage结构体，一个存储系统当然需要相应的存储引擎。而engine的定义已经在util中有了，那么结构体第一个成员就是engine的引用，然后我们再加上定义该存储的配置文件config（未来可能有用）。

定义好结构体后，需要实现一个拿到该实例的方法，便是NewStandAloneStorage。具体实现如下：

```go

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dirPath := conf.DBPath
	kvPath := path.Join(dirPath, "kv")
	raftPath := path.Join(dirPath, "raft")

	// get new db
	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)

	sas := &StandAloneStorage{
		engine: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
	}
	return sas
}
```

对于 start，目前好像没什么用，所以直接返回nil

接下来是重头戏 Reader和Write

##### Reader

project1的描述中给了提示，让我们用到txn，txn就是transaction的缩写。意味着我们要开启一个新事务，我们需要在读数据的时候开启一个新事务的原因是: the transaction handler provided by badger could provide a consistent snapshot of the keys and values. 这样就确保了读取的一致性。

开启事物之后我们就需要思考返回的是一个StorageReader的类型。我们发现它是一个接口，有三个方法GetCF、IterCF、Close，那么我们就需要自己定义一个reader来实现这个接口。

其中reader本质上是一个txn，而三个方法都可以用相应的util中的方法来实现，就不贴代码了:)

##### Writer

ctx参数没有用到，batch实际上是一组数据。查看Modify结构发现Data是一个接口，它可以是Put或者Delete结构。Modify通过断言操作去判断data实际是put还是delete操作的数据。

那么循环取batch中的数据然后利用util中的方法去实现一下就很方便了。主要是要理解modify这个结构。



#### Part2 Raw API

实现了底层engine的读写方法后，我们可以来到当server接收到client通过grpc传递来的请求时，整个系统应该如何去做的状态了。

##### RawGet

rawGet实际上需要我们得到一个reader然后通过reader去读去相应的数据然后，查看签名根据需要返回的参数类型生成返回参数。

##### RawPut / RawDelete

这两个方法都很相似，根据request生成modify的数据，不同操作生成不同类型的，然后放到一个数组中传入到相应方法。根据参数返回。

##### RawScan

这个方法的作用其实就是读取从startKey开始的limit个数个key value对。

本质上是读取操作，所以需要先得到一个reader，然后生成iterator然后去读数据，注意iterator几个方法，Seek()、Valid()、Next()。



#### Test

` make project1`来测试自己的代码。当然也可以在server_test.go中自己来debug查看自己的代码是否编写正确。