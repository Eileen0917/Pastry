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

let inline charToInt c = int c - int '0'

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

let route(dest: string, level: int, func: string, nodeID: string, lenUUID:int, largeLeaf: int list, smallLeaf: int list, rTable: string[,]): string = 
    let mutable found: bool = false
    let mutable next: string = "somebody"
    let mutable nextDec: int = -1
    let mutable destDec = dest |> int
    let mutable currentDec = nodeID |> int
    let mutable mindiff = abs(destDec - currentDec)

    if level = lenUUID then
        next <- null
        found <- true

    //search in leaf table
    if not found then
        if destDec > currentDec then
            if not (List.isEmpty largeLeaf) then
                if func = "join" then
                    if destDec < largeLeaf.[largeLeaf.Length - 1] then
                        for i in 0 .. (largeLeaf.Length - 1) do
                            if largeLeaf.[i] < destDec then 
                                nextDec <- largeLeaf.[i]
                elif func = "route" then
                    if destDec <= largeLeaf.[largeLeaf.Length - 1] then
                        for i in 0 .. (largeLeaf.Length - 1) do
                            if largeLeaf.[i] <= destDec then 
                                nextDec <- largeLeaf.[i]
                if (nextDec <> -1) then
                    next <- getRandomID(nextDec, lenUUID)
                    found <- true
        elif destDec < currentDec then
            if not (List.isEmpty smallLeaf) then
                if func = "join" then
                    if destDec > smallLeaf.[0] then
                        for i in 0 .. (smallLeaf.Length - 1) do
                            if smallLeaf.[i] < destDec then 
                                nextDec <- smallLeaf.[i]
                elif func = "route" then
                    if destDec >= smallLeaf.[0] then
                        for i in 0 .. (smallLeaf.Length - 1) do
                            if smallLeaf.[i] <= destDec then 
                                nextDec <- smallLeaf.[i]
                if (nextDec <> -1) then
                    next <- getRandomID(nextDec, lenUUID)
                    found <- true
    
    // search in route table
    if not found then
        let dl = dest.[level] |> charToInt
        if rTable.[level, dl] <> null then
            if func = "join" then
                // TODO: fix when rTable constructed
                printfn "."
                // if int(rTable.[level, dl]) < destDec then
                //     next <- rTable.[level, dl]
                // else
                //     next <- null
                // found <- true
                        
            elif func = "route" then
                // TODO: fix when rTable constructed
                printfn "."
                // if int(rTable.[level, dl]) <= destDec then
                //     next <- rTable.[level, dl]ㄆ
                //     found <- true
    
    // try to get closer
    if not found then
        let mutable eligibleNodes = new HashSet<int>()

        for i in 0 .. (largeLeaf.Length - 1) do
            let largeleafNode = getRandomID(largeLeaf.[i], lenUUID)
            if (shl(largeleafNode, dest) >= level) then
                eligibleNodes.Add(largeLeaf.[i]) |> ignore
        
        for i in 0 .. (smallLeaf.Length - 1) do
            let smallleafNode = getRandomID(smallLeaf.[i], lenUUID)
            if (shl(smallleafNode, dest) >= level) then
                eligibleNodes.Add(smallLeaf.[i]) |> ignore

        for i in level .. (rTable.[*,0].Length - 1) do
            for j in 0 .. (rTable.[0,*].Length - 1) do
                //TODO: fill rTable so that can add the value to eligibleNodes
                printfn "."
                // eligibleNodes.Add(int(rTable.[i, j])) |> ignore

        for iSet in eligibleNodes do
            if iSet <> destDec then
                let diff = abs(destDec - iSet)
                if diff < mindiff then
                    mindiff <- diff
                    nextDec <- iSet
                    found <- true

        if found then
            next <- getRandomID(nextDec, lenUUID)

    //current node is the last node
    if not found then
        next <- null
        found <- true

    next

  

let pastryNode nID numsReq numsNodes b l lenUUID logBaseB (nodeMailbox:Actor<nodeMessage>) = 
    let selfActor = nodeMailbox.Self
    let bossActor = select ("akka://FSharp/user/boss") system

    let nodeID: string = nID
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
            printfn "[Pastry] StartRouting"
        
        | Join nextNID ->
            printfn "[Pastry] Join %A" nextNID

            let l = shl(nodeID, nextNID)
            select ("akka://FSharp/user/" + nextNID) system <! AddMe(nodeID, rTable, largeLeaf, smallLeaf, 0)


        | Forward (destination, level, noHops)->
            printfn "[Pastry] Forward"

            let mutable nHops = noHops
            //TODO fix the table var to be pointer
            let next = route(destination, level, "route", nodeID, lenUUID, largeLeaf, smallLeaf, rTable)

            if next = null then
                bossActor <! Finished(nHops)
            else
                nHops <- nHops + 1
                let newLevel = shl(destination, next)
                select ("akka://FSharp/user/" + next) system <! Forward(destination, newLevel, nHops)

        
        | AddMe (destination, rT, lLeaf, sLeaf, lev)->
            let mutable isLastHop : bool = false
            let mutable dRT: string[,] = Array2D.zeroCreate<string> lenUUID lenUUID
            dRT <- Array2D.copy rT
            //print "%A" dRT
            let mutable lLT = lLeaf |> List.map (fun x -> x) 
            let mutable sLT = sLeaf |> List.map (fun x -> x) 
            //print "%A" sLT
            let level = shl(destination, nodeID)
            if (smallLeaf.isEmpty && largeLeaf.isEmpty) then
                //ONLY ONE NODE IN NETWORK
                isLastHop <- true
             

            let next = route(destination, level, "join")

            if (next = null) then
                isLastHop <- true

            UpdateLeafT(level, destination)
            UpdateSelfRT(level, destination)

            dRT <- Array2D.copy UpdateNewRT(destination, level, rT, lev) //update one row in route table of destination node
            lLT = UpdateNewLarge(nodeID |> int, destination, lLT) |> List.map (fun x -> x)
            sLT = UpdateNewSmall(nodeID |> int, destination, sLT) |> List.map (fun x -> x)

            for (curr in largeLeaf) do
                lLT = UpdateNewLarge(curr, destination, lLT) |> List.map (fun x -> x)
                sLT = UpdateNewSmall(curr, destination, sLT) |> List.map (fun x -> x)
            
            for (curr in smallLeaf) do
                lLT = UpdateNewLarge(curr, destination, lLT) |> List.map (fun x -> x)
                sLT = UpdateNewSmall(curr, destination, sLT) |> List.map (fun x -> x)
            
            if (not isLastHop) then
                //var nextPeer = context.actorFor("akka://pastry/user/" + next)
                sender <! NextPeer(next, dRT, lLT, sLT, level)

            else 
                sender <! Deliver(dRT, lLT, sLT)
            
            printfn "[Pastry] AddMe"
            // if 1 = 1 then
            //     selfActor <! Deliver
            // else
            //     selfActor <! NextPeer
        
        | Deliver ->
            printfn "[Pastry] Deliver"
            // bossActor <! Joined

        | NextPeer ->
            printfn "[Pastry] NextPeer"
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
            printfn "[Boss] Initialize"
        
        | Joined nid -> 
            count <- count + 1

            // Just use below code to run StartRouting pattern temporarily
            // for p in peerList do
            //     let m:string = "message!"
            //     p <! StartRouting m
            
            if count = numsNodes then  
                // Thread.Sleep(1000)
                for p in peerList do
                    let m:string = "message!"
                    p <! StartRouting m
            else
                selfActor <! Init nodeID
            printfn "[Boss] Joined"
        
        | Init nid ->
            let initNodeid = getRandomID(i, lenUUID)
            idHash.Add(initNodeid) |> ignore
            let peer = spawn system initNodeid (pastryNode initNodeid numsReq numsNodes b l lenUUID logBaseB)
            peerList <- peerList @ [peer]
            i <- i + 1
            // let peer1 = select ("akka://FSharp/user/" + nid) system
            peer <! Join nid
            printfn "[Boss] Init"

        | Finished nHops ->
            printfn "[Boss] Finished"
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
    let numsOfNodes = argv.[3] |> int
    let numRequests = argv.[4] |> int

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