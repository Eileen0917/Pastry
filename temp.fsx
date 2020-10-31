#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open System.Collections.Generic
open Akka.Actor
open Akka.FSharp

type bossMessage = 
    | Initialize
    | Init of string
    | Joined of string
    | Finished of int

type nodeMessage = 
    | FirstJoin of string * int * int * int
    | Join of ActorRef

let system = ActorSystem.Create("FSharp")
// let nodeNamePrefix = "Node"
// let globalStopWatch = System.Diagnostics.Stopwatch()



let pastryNode nodeID numsReq numsNodes b l lenUUID logBaseB (nodeMailbox:Actor<nodeMessage>) = 
    let rec loop () = actor {
        let! (msg: nodeMessage) = nodeMailbox.Receive()
        match msg with 
        | FirstJoin nodeID b l lenUUID->
            printfn "First Join"
        return! loop ()
    } 
    loop ()

let boss numsNodes numsReq (bossMailbox:Actor<bossMessage>) =  
    let b:int = 3   // 2
    let l:int = 16  // 4
    let lenUUID:int = 1 << b
    let logBaseB = int((Math.log(numNodes) / Math.log(lenUUID)))
    let i:int = 1
    let idHash = Set<String>()
    let peerList = List<ActorRef>()
    let prevNID:string = "first"
    let mutable count:int = 0
    let terminateCount:int = 0
    let numN:double = double (numsNodes)
    let numR:double = double (numsReq)
    let mutable totalHops: double = 0
    let nodeID:string = "hi"

    let rec loop numsNodes numsReq = actor {
        let! (msg: bossMessage) = bossMailbox.Receive()
        printfn "i am in boss"
        match msg with
        | Initialize ->
            nodeID = getRandomID(i)
            idHash.Add(nodeID)
            let peer = spawn system nodeID (pastryNode nodeID numsReq numsNodes b l lenUUID logBaseB)
            peerList <- peerList :: peer
            i <- i + 1
            peer <! FirstJoin(nodeID, b, l, lenUUID)
            printf "[Boss Init]"
        | Init nid ->
            let initNodeid = getRandomID(i)
            idHash.Add(initNodeid)
            let peer = spawn system initNodeid (pastryNode initNodeid numsReq numsNodes b l lenUUID logBaseB)
            peerList <- peerList :: peer
            i <- i + 1
            let peer1 = select ("akka://FSharp/user/" + nid) system
            peer <! Join peer1
        | Joined nid ->
            count <- count + 1
            if count = numsNodes then  
                Thread.sleep(1000)
                for p in peerList do
                    let m:string = "message!"
                    p <! StartRouting m
            else
                self <! Init nodeID
        | Finished nHops ->
            terminateCount <- terminateCount + 1
            totalHops <- double nHops
            if terminateCount >= (numsNodes * numsReq) then
                Thread.sleep(1000)
                printfn "All nodes have finished routing ..."
                printfn "Total routes: %d" (numN * numR)
                printfn "Total hops: %d" totalHops
                printfn "Average hops per route: %d" (totalHops / (numN * numR))
                Environment.Exit 1

        return! loop numsNodes numsReq
    }
    loop numsNodes numsReq




let main () =
    let argv = System.Environment.GetCommandLineArgs()
    let numsOfNodes = int(argv.[3])
    let numRequests = int(argv.[4])

    let pastryBoss = spawn system "boss" (boss numsOfNodes numRequests)
    pastryBoss <! START

    0

main ()
