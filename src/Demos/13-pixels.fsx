#load "credentials.fsx"
#load "lib/collections.fsx"

open System
open System.IO

open Nessos.Streams

let trainPath = __SOURCE_DIRECTORY__ + "/../../data/train.csv"
let testPath = __SOURCE_DIRECTORY__ + "/../../data/test.csv"

type Point = int []
type Distance = Point -> Point -> uint64
type TrainingPoint = int * Point // classification x point
type Classifier = TrainingPoint [] -> Point -> int

// parse training points using streams
let parseTraining (path : string) : TrainingPoint [] =
    File.ReadAllLines path
    |> Stream.ofArray
    |> Stream.skip 1
    |> Stream.map (fun line -> line.Split(','))
    |> Stream.map (fun line -> line |> Array.map int)
    |> Stream.map (fun line -> line.[0], line.[1..])
    |> Stream.toArray

// parse points using streams
let parsePoints (path : string) : Point [] =
    File.ReadAllLines path
    |> Stream.ofArray
    |> Stream.skip 1
    |> Stream.map (fun line -> line.Split(','))
    |> Stream.map (fun line -> line |> Array.map int)
    |> Stream.toArray

// separating training & validation set
let training : TrainingPoint [] = parseTraining trainPath
let test : Point [] = parsePoints testPath
 
// l^2 distance 
let l2 : Distance =
    fun x y ->
        let mutable acc = 0uL
        for i = 0 to x.Length - 1 do
            acc <- acc + uint64 (pown (x.[i] - y.[i]) 2)
        acc

// single-threaded, stream-based k-nearest neighbour classifier
let knn (d : Distance) (k : int) : Classifier =
    fun (training : TrainingPoint []) (point : Point) ->
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

// local multicore classification
let classifyLocalMulticore (classifier : Classifier) (training : TrainingPoint []) (points : Point []) =
    Array.Parallel.map (classifier training) points

// local multicore validation
let validateLocalMulticore (classifier : Classifier) (training : TrainingPoint []) (validation : TrainingPoint []) =
    ParStream.ofArray validation
    |> ParStream.map(fun (expected,point) -> expected, classifier training point)
    |> ParStream.map(fun (expected,prediction) -> if expected = prediction then 1. else 0.)
    |> ParStream.sum
    |> fun results -> results / float validation.Length

#time

// Performance (Quad core i7 CPU)
// Real: 00:01:02.281, CPU: 00:07:51.481, GC gen0: 179, gen1: 82, gen2: 62
let training' = training.[ .. 39999]
let validation = training.[40000 ..]
validateLocalMulticore classifier training' validation

// Real: 00:15:30.855, CPU: 01:56:59.842, GC gen0: 2960, gen1: 2339, gen2: 1513
classifyLocalMulticore classifier training test

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

let evaluateDistributed (classifier : Classifier) (training : CloudSequence<TrainingPoint>) (validation : TrainingPoint []) = cloud {
    let evaluateSingleThreaded (validation : TrainingPoint []) = local {
        let! _ = trainingRef.PopulateCache() // cache to local memory for future use
        let! training = trainingRef.ToArray()
        return
            validation
            |> Stream.ofArray
            |> Stream.map (fun (expected,point) -> if expected = classifier training point then 1 else 0)
            |> Stream.sum
    }

    let! successful = validation |> Distributed.reduceCombine evaluateSingleThreaded (Local.lift Array.sum)
    return float successful / float validation.Length
}

let job = evaluateDistributed classifier trainingRef validation |> cluster.CreateProcess

job.AwaitResult()