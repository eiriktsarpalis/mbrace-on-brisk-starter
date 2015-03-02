#load "credentials.fsx"
#load "lib/collections.fsx"

open System
open System.IO

open Nessos.Streams

let path = __SOURCE_DIRECTORY__ + "/../../data/train.csv"

type Point = int []
type Distance = Point -> Point -> int
type TrainingSet = (int * Point) []
type Classifier = TrainingSet -> Point -> int

// read using streams
let data : TrainingSet =
    File.ReadAllLines path
    |> Stream.ofArray
    |> Stream.skip 1
    |> Stream.map (fun line -> line.Split(','))
    |> Stream.map (fun line -> line |> Array.map int)
    |> Stream.map (fun line -> line.[0], line.[1..])
    |> Stream.toArray

// separating training & validation set
let training : TrainingSet = data.[..39999]
let validation : TrainingSet = data.[40000..]
 
// l^2 distance 
let l2 : Distance = Array.fold2 (fun acc x y -> acc + pown (x-y) 2) 0

// single-threaded, stream-based k-nearest neighbour classifier
let knn (d : Distance) (k : int) : Classifier =
    fun (training : TrainingSet) (point : Point) ->
        training
        |> Stream.ofArray
        |> Stream.sortBy (fun ex -> d (snd ex) point)
        |> Stream.take k
        |> Stream.map fst
        |> Stream.countBy id
        |> Stream.maxBy snd
        |> fst

// classifier instance used for this example
let classifier = knn l2 10


// local multicore evaluation
let evaluateLocalMulticore () =
    ParStream.ofArray validation
    |> ParStream.map(fun (expected,point) -> expected, classifier training point)
    |> ParStream.map(fun (expected,prediction) -> if expected = prediction then 1. else 0.)
    |> ParStream.sum
    |> fun results -> results / float validation.Length

#time
// Performance (Quad core i7 CPU)
// Real: 00:01:25.783, CPU: 00:10:51.647, GC gen0: 153, gen1: 73, gen2: 43
evaluateLocalMulticore()


//
//  Section: MBrace
//

open MBrace
open MBrace.Store
open MBrace.Workflows
open MBrace.Azure.Client

// First connect to the cluster
let cluster = Runtime.GetHandle(config)

// Save training set as cloud reference in Azure store, returning a typed reference to data
let trainingRef = cluster.DefaultStoreClient.CloudSequence.New training

//// work around store performance bug using this monstrosity: (see also https://github.com/mbraceproject/MBrace.Azure/issues/26)
//let createCloudSequence(ts : seq<'T>) =
//    let serialize (config : Store.CloudFileStoreConfiguration) = async {
//        let localPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName())
//        let fs = File.OpenWrite localPath 
//        let _ = config.Serializer.SeqSerialize(fs, ts, false)
//        use fs = File.OpenRead localPath
//        let storePath = config.FileStore.GetRandomFilePath(config.DefaultDirectory)
//        do! config.FileStore.OfStream(fs, storePath)
//        return storePath
//    }
//
//    cloud {
//        let! config = Continuation.Cloud.GetResource<Store.CloudFileStoreConfiguration> ()
//        let! store = Cloud.OfAsync <| serialize config
//        return! CloudSequence.Parse<'T>(store, serializer = config.Serializer)
//    }
//
//let trainingRef = training |> createCloudSequence |> cluster.RunLocal

let evaluateDistributed (inputs : (int * Point) []) = cloud {
    let evaluateSingleThreaded (inputs : (int * Point) []) = cloud {
        let! _ = trainingRef.Cache() // cache to local memory for future use
        let! training = trainingRef.ToArray()
        return
            inputs
            |> Stream.ofArray
            |> Stream.map (fun (expected,point) -> if expected = classifier training point then 1 else 0)
            |> Stream.sum
    }

    let! successful = inputs |> Distributed.reduceCombine evaluateSingleThreaded (Cloud.lift Array.sum)
    return float successful / float inputs.Length
}

let job = validation |> evaluateDistributed |> cluster.CreateProcess