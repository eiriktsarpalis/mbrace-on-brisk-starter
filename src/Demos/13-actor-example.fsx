#load "credentials.fsx"
#load "lib/collections.fsx"

open MBrace
open MBrace.Azure
open MBrace.Azure.Client
open MBrace.Azure.Runtime
open MBrace.Streams
open MBrace.Workflows
open Nessos.Streams

(**
 This tutorial demonstrates a simple actor library implemented on top of MBrace
 
 Before running, edit credentials.fsx to enter your connection strings.
**)

// First connect to the cluster
let cluster = Runtime.GetHandle(config)


/// Actor message inbox
type Mailbox<'T> = IReceivePort<'T>
/// Actor reply channel
type ReplyChannel<'T> = ISendPort<'T>

/// Cloud actor instance
type CloudActor<'T> =
    {
        Sender : ISendPort<'T>
        Task : ICloudTask<unit>
        CancellationTokenSource : ICloudCancellationTokenSource
    }
with
    /// Post message to actor
    member a.Post(msg : 'T) = cloud {
        return! a.Sender.Send msg
    }

    /// Post message to actor, awaiting for reply
    member a.PostWithReply(messageBuilder : ReplyChannel<'R> -> 'T) = cloud {
        let! sp,rp = CloudChannel.New<'R> ()
        use rp = rp
        do! a.Post(messageBuilder sp)
        return! rp.Receive()
    }

    /// Kills the actor instance
    member a.Kill () = a.CancellationTokenSource.Cancel()

    interface ICloudDisposable with
        member a.Dispose () = cloud { a.CancellationTokenSource.Cancel() }

module CloudActor =
    /// Deploys a new actor instance with provided behaviour on cluster
    let create (behaviour : Mailbox<'T> -> Cloud<unit>) : Cloud<CloudActor<'T>> = cloud {
        let! cts = Cloud.CreateCancellationTokenSource()
        let! sp, rp = CloudChannel.New<'T> ()
        let! task = Cloud.StartAsCloudTask(behaviour rp, faultPolicy = FaultPolicy.NoRetry, cancellationToken = cts.Token)
        return {
            Sender = sp
            Task = task
            CancellationTokenSource = cts
        }
    }

// sample implementation : counter

type Counter =
    | IncrementBy of int
    | GetValue of ReplyChannel<int>
    | Reset of int

let rec behaviour (state : int) (mbox : Mailbox<Counter>) = cloud {
    let! msg = mbox.Receive()
    match msg with
    | IncrementBy i -> return! behaviour (state + i) mbox
    | Reset c -> return! behaviour c mbox
    | GetValue rc -> 
        do! rc.Send state
        return! behaviour state mbox
}

// deploy actor to cluster
let actor = CloudActor.create (behaviour 0) |> cluster.Run

// local interaction with actor
actor.Post (IncrementBy 1) |> cluster.RunLocal
actor.Post (IncrementBy 2) |> cluster.RunLocal
actor.Post (IncrementBy 3) |> cluster.RunLocal

actor.PostWithReply GetValue |> cluster.RunLocal

// cluster interaction with actor
actor.Post (Reset 0) |> cluster.RunLocal

[1 .. 100]
|> Distributed.map (actor.Post << IncrementBy)
|> Cloud.Ignore
|> cluster.Run

actor.PostWithReply GetValue |> cluster.RunLocal