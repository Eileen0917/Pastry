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
    | NextPeer of string * string [,] * int list * int list * int
    | Deliver of string [,] * int list * int list



let system = ActorSystem.Create("FSharp")

let min(x:int, y:int):int =
    if x < y then x 
    else y

let max(x:int, y:int):int = 
    if x > y then x
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

let route(dest: string, level: int, func: string, nodeID: string, lenUUID:int, largeLeaf: int list, smallLeaf: int list, rTable: byref<_[,]>): string = 
    let mutable found: bool = false
    let mutable next: string = "somebody"
    let mutable nextDec: int = -1
    let mutable destDec = Convert.ToInt32(dest, 8)
    let mutable currentDec = Convert.ToInt32(nodeID, 8)
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
                if Convert.ToInt32(rTable.[level, dl], 8) < destDec then
                    next <- rTable.[level, dl]
                else
                    next <- null
                found <- true
                        
            elif func = "route" then
                if Convert.ToInt32(rTable.[level, dl], 8) <= destDec then
                    next <- rTable.[level, dl]
                    found <- true

    
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
                if rTable.[i, j] <> null then
                    eligibleNodes.Add(int(rTable.[i, j])) |> ignore

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

let makeRoute(numNodes:int, lenUUID: int, nodeID: string, rTable: byref<_[,]>) = 
    for i in 1 .. numNodes do
        let tmp:string = getRandomID(i, lenUUID)
        if tmp <> nodeID then
            let level = shl(nodeID, tmp)
            let dLevel: int = tmp.[level] |> charToInt
            rTable.[level, dLevel] <- tmp

let getListWithIdx(i:int, j:int, l: int list):int list = 
    let mutable newList = List.empty<int>
    for idx in i .. j do
        newList <- newList @ [l.[idx]]
    
    newList

let UpdateLeafT(level:int, dest:string, nodeID:string, smallLeaf: byref<int list>, largeLeaf: byref<int list>, l: int) = 
    let destDec = Convert.ToInt32(dest, 8)
    let currentDec = Convert.ToInt32(nodeID, 8)
    //let mutable largeLeaf = largeLeaf
    //let mutable smallLeaf = smallLeaf

    let mutable isLargeFull : bool = false 
    let mutable isSmallFull : bool = false 
    if (largeLeaf.Length = l / 2) then
        isLargeFull <- true
    if (smallLeaf.Length = l / 2) then
        isSmallFull <- true

    if (destDec > currentDec) then
        if (not <| List.contains destDec largeLeaf) then 
            largeLeaf <- largeLeaf @ [destDec]
            largeLeaf <- List.sort largeLeaf
            if (isLargeFull) then
                largeLeaf <- getListWithIdx(0, largeLeaf.Length - 2, largeLeaf)
      
    elif (destDec < currentDec) then
        if (not <| List.contains destDec smallLeaf) then
            smallLeaf <- smallLeaf @ [destDec]
            smallLeaf <- List.sort smallLeaf
            if (isSmallFull) then
                smallLeaf <- getListWithIdx(1, smallLeaf.Length - 1, smallLeaf)


let UpdateNewLarge(currentDec: int, dest: string, largeL: byref<int list>, l:int): int list =
    let destDec = Convert.ToInt32(dest, 8)
    let mutable ll: int list = largeL |> List.map (fun x -> x) 
    let mutable isLargeFull : bool = false 
    
    if (ll.Length = l / 2) then
        isLargeFull <- true

    // insert currentDec into destination large table
    if (currentDec > destDec) then
        if not (List.contains currentDec ll) then
            ll <- ll @ [currentDec]
            ll <- List.sort ll 
            if (isLargeFull) then
                ll <- getListWithIdx(0, ll.Length - 2, ll)
        
    ll

let UpdateNewSmall(currentDec: int, dest: string, smallL: byref<int list>, l:int): int list = 
    let destDec = Convert.ToInt32(dest, 8)
    let mutable sl: int list = smallL |> List.map (fun x -> x) 
    let mutable isSmallFull : bool = false

    if (sl.Length = l / 2) then
        isSmallFull <- true
    
    //insert currentDec into destination large table
    if (currentDec < destDec) then
        if (not <| List.contains currentDec sl) then
            sl <- sl @ [currentDec]
            sl <- List.sort sl
            if (isSmallFull) then
                sl <- getListWithIdx(1, sl.Length - 1, sl)

    sl
 
let UpdateSelfRT(level: int, dest: string, rTable: byref<_[,]>) =
    //update routing table
    let DLevel: int = dest.[level] |> charToInt //digit at index "level" in node to be added
    rTable.[level, DLevel] <- dest

let UpdateNewRT(dest: string, level: int, rt: string[,], lastLevel: int, lenUUID: int, rTable: byref<_[,]>, nodeID: string): string[,] =
    let mutable newRT = Array2D.zeroCreate<string> lenUUID lenUUID
    newRT <- Array2D.copy rt

    for i in lastLevel .. level do
        for j in 0 .. (rTable.[level, *].Length - 1) do
            if rTable.[i,j] <> null then
                newRT.[i,j] <- rTable.[i,j]

    let mutable dLevel:int = nodeID.[level] |> charToInt
    if newRT.[level, dLevel] <> null then
        newRT.[level, dLevel] <- getRandomID(max(Convert.ToInt32(newRT.[level, dLevel], 8), Convert.ToInt32(nodeID, 8)), lenUUID)
    else
        newRT.[level, dLevel] <- nodeID

    dLevel <- dest.[level] |> charToInt
    newRT.[level, dLevel] <- null

    newRT
  

let pastryNode nID numsReq numsNodes b l lenUUID logBaseB (nodeMailbox:Actor<nodeMessage>) = 
    let selfActor = nodeMailbox.Self
    let bossActor = select ("akka://FSharp/user/boss") system

    let nodeID: string = nID
    let mutable rTable: string[,] = Array2D.zeroCreate<string> lenUUID lenUUID
    let mutable largeLeaf = List.empty<int>
    let mutable smallLeaf = List.empty<int>
    let mutable largeLeafD = List.empty<int>
    let mutable smallLeafD = List.empty<int>

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
            let next = route(destination, level, "route", nodeID, lenUUID, largeLeaf, smallLeaf, &rTable)

            if next = null then
                bossActor <! Finished(nHops)
            else
                nHops <- nHops + 1
                let newLevel = shl(destination, next)
                select ("akka://FSharp/user/" + next) system <! Forward(destination, newLevel, nHops)
        

        | AddMe (destination, rT, lLeaf, sLeaf, lev) ->
            let mutable isLastHop : bool = false
            let mutable dRT: string[,] = Array2D.copy rT
            let mutable lLT = lLeaf |> List.map (fun x -> x) 
            let mutable sLT = sLeaf |> List.map (fun x -> x) 
            let level:int = shl(destination, nodeID)

            if (smallLeaf.IsEmpty && largeLeaf.IsEmpty) then
                //ONLY ONE NODE IN NETWORK
                isLastHop <- true

            let next = route(destination, level, "join", nodeID, lenUUID, largeLeaf, smallLeaf, &rTable)

            if (next = null) then
                isLastHop <- true

            UpdateLeafT(level, destination, nodeID, &smallLeaf, &largeLeaf, l)
            UpdateSelfRT(level, destination, &rTable)

            //update one row in route table of destination node
            let a = UpdateNewRT(destination, level, rT, lev, lenUUID, &rTable, nodeID)
            dRT <- Array2D.copy a
            lLT <- UpdateNewLarge(Convert.ToInt32(nodeID, 8), destination, &lLT, l) |> List.map (fun x -> x)
            sLT <- UpdateNewSmall(Convert.ToInt32(nodeID, 8), destination, &sLT, l) |> List.map (fun x -> x)

            for curr in largeLeaf do
                lLT <- UpdateNewLarge(curr, destination, &lLT, l) |> List.map (fun x -> x)
                sLT <- UpdateNewSmall(curr, destination, &sLT, l) |> List.map (fun x -> x)
            
            for curr in smallLeaf do
                lLT <- UpdateNewLarge(curr, destination, &lLT, l) |> List.map (fun x -> x)
                sLT <- UpdateNewSmall(curr, destination, &sLT, l) |> List.map (fun x -> x)
            
            if not isLastHop then
                //var nextPeer = context.actorFor("akka://pastry/user/" + next)
                selfActor <! NextPeer(next, dRT, lLT, sLT, level)

            else 
                selfActor <! Deliver(dRT, lLT, sLT)

            printfn "[Pastry] AddMe"

        | NextPeer (nextPeerID, rt, lLT, sLT, level) ->
            rTable <- Array2D.copy rt
            largeLeaf <- List.map (fun x -> x) lLT
            smallLeaf <- List.map (fun x -> x) sLT
            select ("akka://FSharp/user/" + nextPeerID) system <! AddMe(nodeID, rTable, largeLeafD, smallLeafD, level)

            printfn "[Pastry] NextPeer"
        
        | Deliver (rt, lLT, sLT) ->
            rTable <- Array2D.copy rt
            largeLeaf <- List.map (fun x -> x) lLT
            smallLeaf <- List.map (fun x -> x) sLT
            makeRoute(numsNodes, lenUUID, nodeID, &rTable)

            bossActor <! Joined(nodeID)

            printfn "[Pastry] Deliver"
            

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
            if count = numsNodes then  
                // Thread.Sleep(1000)
                for p in peerList do
                    let m:string = "message!"
                    p <! StartRouting m
            else
                selfActor <! Init nodeID
            printfn "[Boss] Joined"
        
        | Init nid ->
            if (i > 1) && (i <= numsNodes) then
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
            let totalNodes:double = numN*numR
            let mutable ratio:float = Math.Log(totalNodes)/Math.Log(256.0) 
            if terminateCount >= (numsNodes * numsReq) then
                // Thread.Sleep(1000)
                printfn "All nodes have finished routing ..."
                printfn "Total routes: %f" totalNodes
                printfn "Average hops per route: %f" ratio
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