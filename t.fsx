#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open System.Collections.Generic
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

type bossMessage = 
    | Initialize
    | Init of string
    | Joined of string
    | Finished of int

type nodeMessage = 
    | FirstJoin of string
    | StartRouting of string
    | Join of string
    | Forward of string * int * int
    | AddMe of string * string [,] * int list * int list * int
    | NextPeer
    | Deliver


let system = ActorSystem.Create("FSharp")

let min(x:int, y:int):int =
    if x < y then x 
    else y

let getRandomID(i:int, lenUUID: int):string = 
    let mutable sb = ""
    let mutable strZ = ""

    sb <- sb + Convert.ToString(i, 8)
    for i in sb.Length .. (lenUUID - 1) do
        strZ <- strZ + Convert.ToString(0, 8)
    strZ <- strZ + sb

    strZ

let shl(nID1:string, nID2:string):int =
    let maxSize = min(nID1.Length, nID2.Length)
    let mutable i:int = 0

    while i < maxSize && nID1.[i] = nID2.[i] do
        i <- i + 1

    i

let createRandomString(numsOfNodes:int, lenUUID:int):string = 
    let r = Random().Next(numsOfNodes) + 1
    let mutable sb = ""
    let mutable strZ = ""
    let flag:bool = true

    sb <- sb + Convert.ToString(r, 8)
    for i in sb.Length .. (lenUUID - 1) do
        strZ <- strZ + Convert.ToString(0, 8)
    
    strZ <- strZ + sb

    strZ

    

let pastryNode nID numsReq numsNodes b l lenUUID logBaseB (nodeMailbox:Actor<nodeMessage>) = 
    let selfActor = nodeMailbox.Self
    let bossActor = select ("akka://FSharp/user/boss") system

    let nodeID: String = nID
    let rTable: string[,] = Array2D.zeroCreate<string> lenUUID lenUUID
    let largeLeaf = List.empty<int>
    let smallLeaf = List.empty<int>
    let largeLeafD = List.empty<int>
    let smallLeafD = List.empty<int>

    let rec loop () = actor {    
        let! (msg: nodeMessage) = nodeMailbox.Receive()
        match msg with 
        | FirstJoin nid ->
            nodeMailbox.Sender() <! Joined nid
        
        | StartRouting m ->
            for i in 1 .. numsReq do
            let key:string = createRandomString(numsNodes, lenUUID)
            let level = shl(key, nodeID)
            selfActor <! Forward (key, level, 0)
        
        | Join nextNID ->
            printfn "Join %A" nextNID

            let l = shl(nodeID, nextNID)
            select ("akka://FSharp/user/" + nextNID) system <! AddMe(nodeID, rTable, largeLeaf, smallLeaf, 0)


        | Forward (destination, level, noHops)->
            printfn "Forward"
            // if 1 <> 1 then
            //     printfn "forward"
            // else
            //     bossActor <! Finished
        
        | AddMe (destination, rT, lLeaf, sLeaf, lev)->
            printfn "AddMe"
            // if 1 = 1 then
            //     selfActor <! Deliver
            // else
            //     selfActor <! NextPeer
        
        | Deliver ->
            printfn "Deliver"
            // bossActor <! Joined

        | NextPeer ->
            printfn "NextPeer"
            // selfActor <! AddMe

        return! loop ()
    } 
    loop ()


let boss numsNodes numsReq (bossMailbox:Actor<bossMessage>) = 
    let selfActor = bossMailbox.Self

    let b:int = 3
    let l:int = 16
    let lenUUID:int = 1 <<< b
    let logBaseB:int = int(Math.Log(float(numsNodes), float(lenUUID)))
    let mutable i:int = 1
    let idHash = new HashSet<string>()
    let mutable peerList= List.empty
    let mutable prevNID:string = "first"
    let mutable count:int = 0
    let mutable terminateCount:int = 0
    let numN:double = double (numsNodes)
    let numR:double = double (numsReq)
    let mutable totalHops: double = 0.0
    let mutable nodeID:string = "hi"

    let rec loop () = actor {
        let! (msg: bossMessage) = bossMailbox.Receive()
        match msg with
        | Initialize ->
            nodeID <- getRandomID(i, lenUUID)
            idHash.Add(nodeID) |> ignore
            let peer = spawn system nodeID (pastryNode nodeID numsReq numsNodes b l lenUUID logBaseB)
            peerList <- peerList @ [peer]
            i <- i + 1
            peer <! FirstJoin nodeID
            printfn "[Boss Initialize]"
        
        | Joined nid -> 
            count <- count + 1
            if count = numsNodes then  
                // Thread.Sleep(1000)
                for p in peerList do
                    let m:string = "message!"
                    p <! StartRouting m
            else
                selfActor <! Init nodeID
        
        | Init nid ->
            let initNodeid = getRandomID(i, lenUUID)
            idHash.Add(initNodeid) |> ignore
            let peer = spawn system initNodeid (pastryNode initNodeid numsReq numsNodes b l lenUUID logBaseB)
            peerList <- peerList @ [peer]
            i <- i + 1
            // let peer1 = select ("akka://FSharp/user/" + nid) system
            peer <! Join nid
            printfn "[Boss Init]"

        | Finished nHops ->
            printfn "[Boss Finished]"
            terminateCount <- terminateCount + 1
            totalHops <- double nHops
            if terminateCount >= (numsNodes * numsReq) then
                // Thread.Sleep(1000)
                printfn "All nodes have finished routing ..."
                printfn "Total routes: %f" (numN * numR)
                printfn "Total hops: %f" totalHops
                printfn "Average hops per route: %f" (totalHops / (numN * numR))
                Environment.Exit 1  

        return! loop ()
    }

    loop ()

let main () =
    let argv = System.Environment.GetCommandLineArgs()
    let numsOfNodes = int(argv.[3])
    let numRequests = int(argv.[4])

    for timeout in [1000000] do
        try
            let pastryBoss = spawn system "boss" (boss numsOfNodes numRequests) 
            let task = (pastryBoss <? Initialize)
            Async.RunSynchronously (task, timeout)

        with :? TimeoutException ->
            printfn "ask: timeout!"

    // let pastryBoss = spawn system "boss" (boss numsOfNodes numRequests)
    // pastryBoss <! Initialize

    0

main ()