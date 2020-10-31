open System
open Akka.Actor
open Akka.FSharp

type bossMessage = 
    | START 
    | QQ of int

let system = ActorSystem.Create("FSharp")
let nodeNamePrefix = "Node"
let globalStopWatch = System.Diagnostics.Stopwatch()




// let boss numsNodes numsReq (bossMailbox:Actor<bossMessage>) =  
//     let rec loop numsNodes numsReq = actor {
//         let! (msg: bossMessage) = bossMailbox.Receive()
//         printfn "i am in boss"
//         match msg with
//         | START ->
//             printfn "[Boss Init]"
//         | QQ ->
//             printfn "QQ"

//         printfn "ff"  
//         return! loop numsNodes numsReq
//     }
//     loop numsNodes numsReq

let boss numsNodes numsReq (bossMailbox:Actor<bossMessage>) =  
    printfn "%i %i" numsNodes numsReq
    let rec loop numsNodes numsReq = actor {
        let! (msg: bossMessage) = bossMailbox.Receive()
        printfn "i am in boss"
        match msg with
        | START ->
            printf "[Boss Init]"
        | QQ x ->
            printf "QQ %i" x

        printfn "ff"  
        return! loop numsNodes numsReq
    }
    loop numsNodes numsReq
let q2q = printfn "FF"

[<EntryPoint>]
let main argv =
    try 
        let numsOfNodes = int(argv.[0])
        let numRequests = int(argv.[1])

        

        let aa = spawn system "boss" (boss numsOfNodes numRequests)
        aa <! QQ 3
        q2q
    with 
    | :? IndexOutOfRangeException ->
        printfn "\n[Main] Incorrect Inputs or IndexOutOfRangeException!\n"

    | :?  FormatException ->
        printfn "\n[Main] FormatException!\n"
    // for timeout in [1000000] do
    //     try
    //         let task = (boss <? START (numsOfNodes, numRequests, alg))
    //         Async.RunSynchronously (task, timeout)

    //     with :? TimeoutException ->
    //         printfn "ask: timeout!"
    0 // return an integer exit code
