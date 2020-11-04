#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open System.Collections.Generic
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

type bossMessage = 
    | Initialize
    | Init of string
    | Joined of string
    | Finished of int
    | Trigger

type nodeMessage = 
    | FirstJoin of string
    | StartRouting of string
    | Join of string
    | Forward of string * int * int
    | AddMe of string * string [,] * int list * int list * int
    | NextPeer of string * string [,] * int list * int list * int
    | Deliver of string [,] * int list * int list

let system = ActorSystem.Create("FSharp")
let b : int = 3
let l : int = 16
let nodeIdLen: int = 1 <<< b

let min(x:int, y:int):int =
    if x < y then x 
    else y

let max(x:int, y:int):int = 
    if x > y then x
    else y

let inline charToInt c = int c - int '0'

let getRandomID(idx:int):string = 
    let mutable sb = ""
    let mutable strZ = ""

    sb <- sb + Convert.ToString(idx, 8)
    for i in sb.Length .. (nodeIdLen - 1) do
        strZ <- strZ + Convert.ToString(0, 8)
    strZ <- strZ + sb

    strZ

let shl(nID1:string, nID2:string):int =
    let maxSize = min(nID1.Length, nID2.Length)
    let mutable i:int = 0

    while i < maxSize && nID1.[i] = nID2.[i] do
        i <- i + 1

    i

let route(curr: string, dest: string, level: int, func: string, largeLeaf: int list, smallLeaf: int list, rTable: byref<_[,]>): string = 
    let mutable found: bool = false
    let mutable next: string = "next"
    let mutable nextDec: int = -1
    let mutable destDec = Convert.ToInt32(dest, nodeIdLen)
    let mutable currDec = Convert.ToInt32(curr, nodeIdLen)
    let mutable minDiff = abs(destDec - currDec)

    if level = nodeIdLen then
        next <- null
        found <- true

    //search in leaf table
    if not found then
        if destDec > currDec then
            if largeLeaf.Length <> 0 then
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
                next <- getRandomID(nextDec)
                found <- true
        elif destDec < currDec then
            if smallLeaf.Length <> 0 then
                if func = "join" then
                    if destDec > smallLeaf.[smallLeaf.Length - 1] then
                        for i in 0 .. (smallLeaf.Length - 1) do
                            if smallLeaf.[i] < destDec then 
                                nextDec <- smallLeaf.[i]
                elif func = "route" then
                    if destDec >= smallLeaf.[smallLeaf.Length - 1] then
                        for i in 0 .. (smallLeaf.Length - 1) do
                            if smallLeaf.[i] <= destDec then 
                                nextDec <- smallLeaf.[i]
                
            if (nextDec <> -1) then
                next <- getRandomID(nextDec)
                found <- true
    
    // search in route table
    if not found then
        let dl = dest.[level] |> charToInt
        if rTable.[level, dl] <> null then
            let decId: int = Convert.ToInt32(rTable.[level, dl], nodeIdLen)
            if func = "join" then
                if decId < destDec then
                    next <- rTable.[level, dl]
                else
                    next <- null
                found <- true
                        
            elif func = "route" then
                if decId <= destDec then
                    next <- rTable.[level, dl]
                    found <- true
    
    // try to get closer
    if not found then
        let mutable eligibleNodes = List.empty<int>

        for i in 0 .. (largeLeaf.Length - 1) do
            let largeleafNode = getRandomID(largeLeaf.[i])
            if (shl(largeleafNode, dest) >= level) then
                eligibleNodes <- eligibleNodes @ [largeLeaf.[i]]
        
        for i in 0 .. (smallLeaf.Length - 1) do
            let smallleafNode = getRandomID(smallLeaf.[i])
            if (shl(smallleafNode, dest) >= level) then
                eligibleNodes <- eligibleNodes @ [smallLeaf.[i]]

        for i in 0 .. (rTable.[*,0].Length - 1) do
            for j in 0 .. (rTable.[0,*].Length - 1) do
                if rTable.[i, j] <> null then
                    let decRT: int = Convert.ToInt32(rTable.[i,j], nodeIdLen)
                    eligibleNodes <- eligibleNodes @ [decRT]

        for iSet in eligibleNodes do
            if iSet <> destDec then
                let diff:int = abs(destDec - iSet)
                if diff < minDiff then
                    minDiff <- diff
                    nextDec <- iSet
                    found <- true

        if found then
            next <- getRandomID(nextDec)

    //current node is the last node
    if not found then
        next <- null
        found <- true

    next

let getListWithIdx(i:int, j:int, l: int list):int list = 
    let mutable newList = List.empty<int>
    for idx in i .. j do
        newList <- newList @ [l.[idx]]
    
    newList

let updateLeafT(curr:string, dest:string, level:int, largeLeaf: byref<int list>, smallLeaf: byref<int list>) = 
    let destDec = Convert.ToInt32(dest, nodeIdLen)
    let currDec = Convert.ToInt32(curr, nodeIdLen)
    let mutable isLargeFull: bool = false 
    let mutable isSmallFull: bool = false 

    if (largeLeaf.Length = l / 2) then
        isLargeFull <- true
    if (smallLeaf.Length = l / 2) then
        isSmallFull <- true

    if (destDec > currDec) then
        if (not <| List.contains destDec largeLeaf) then 
            largeLeaf <- largeLeaf @ [destDec]
            largeLeaf <- List.sort largeLeaf
            if (isLargeFull) then
                largeLeaf <- getListWithIdx(0, largeLeaf.Length - 2, largeLeaf)
      
    elif (destDec < currDec) then
        if (not <| List.contains destDec smallLeaf) then
            smallLeaf <- smallLeaf @ [destDec]
            smallLeaf <- List.sort smallLeaf
            if (isSmallFull) then
                smallLeaf <- getListWithIdx(1, smallLeaf.Length - 1, smallLeaf)

let updateNewLarge(currDec: int, dest: string, largeL: byref<int list>) =
    let destDec = Convert.ToInt32(dest, nodeIdLen)
    let mutable isLargeFull: bool = false 
    
    if (largeL.Length = l / 2) then
        isLargeFull <- true

    // insert currDec into destination large table
    if (currDec > destDec) then
        if not (List.contains currDec largeL) then
            largeL <- largeL @ [currDec]
            largeL <- List.sort largeL
            if (isLargeFull) then
                largeL <- getListWithIdx(0, largeL.Length - 2, largeL)

let updateNewSmall(currDec: int, dest: string, smallL: byref<int list>) = 
    let destDec = Convert.ToInt32(dest, 8)
    let mutable isSmallFull : bool = false

    if (smallL.Length = l / 2) then
        isSmallFull <- true
    
    //insert currentDec into destination large table
    if (currDec < destDec) then
        if (not <| List.contains currDec smallL) then
            smallL <- smallL @ [currDec]
            smallL <- List.sort smallL
            if (isSmallFull) then
                smallL <- getListWithIdx(1, smallL.Length - 1, smallL)
 
let updateSelfRT(dest: string, level: int, rTable: byref<_[,]>) =
    //update routing table
    let dl: int = dest.[level] |> charToInt
    rTable.[level, dl] <- dest

let updateNewRT(curr: string, dest: string, level: int, rTable: byref<_[,]>) =
    let mutable dl:int = curr.[level] |> charToInt
    
    if rTable.[level, dl] <> null then
        let decRT = Convert.ToInt32(rTable.[level, dl], nodeIdLen)
        let decCurr = Convert.ToInt32(curr, nodeIdLen)
        if decRT < decCurr then
            rTable.[level, dl] <- curr
    else
        rTable.[level, dl] <- curr

    dl <- dest.[level] |> charToInt
    rTable.[level, dl] <- null
  

let pastryNode nID numsReq numsNodes (nodeMailbox:Actor<nodeMessage>) = 
    let selfActor = nodeMailbox.Self
    let bossActor = select ("akka://FSharp/user/boss") system

    let nodeID: string = nID
    let mutable rTable: string[,] = Array2D.zeroCreate<string> nodeIdLen nodeIdLen
    let mutable largeLeaf = List.empty<int>
    let mutable smallLeaf = List.empty<int>

    let rec loop () = actor {    
        let! (msg: nodeMessage) = nodeMailbox.Receive()
        match msg with 
        | FirstJoin nid ->
            nodeMailbox.Sender() <! Joined nid
        
        | StartRouting m ->
            for i in 1 .. (numsReq - 1) do
                let key:string = getRandomID(i)
                let level = shl(key, nodeID)
                selfActor <! Forward (key, level, 0)
        
        | Join nextNID ->
            select ("akka://FSharp/user/" + nextNID) system <! AddMe(nodeID, rTable, largeLeaf, smallLeaf, 0)


        | Forward (destination, level, noHops)->
            let mutable nHops = noHops
            let next = route(nodeID, destination, level, "route", largeLeaf, smallLeaf, &rTable)

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

            let next = route(nodeID, destination, level, "join", largeLeaf, smallLeaf, &rTable)

            if (next = null) then
                isLastHop <- true

            updateLeafT(nodeID, destination,level, &largeLeaf, &smallLeaf)
            updateSelfRT(destination, level, &rTable)

            for i in lev .. level do
                for j in 0 .. (rTable.[level, *].Length - 1) do
                    if rTable.[i,j] <> null then
                        dRT.[i,j] <- rTable.[i,j]

            updateNewRT(nodeID, destination, level, &dRT)

            let nodeDec: int = Convert.ToInt32(nodeID, nodeIdLen)
            updateNewLarge(nodeDec, destination, &lLT)
            updateNewSmall(nodeDec, destination, &sLT)

            for curr in largeLeaf do
                updateNewLarge(curr, destination, &lLT)
                updateNewSmall(curr, destination, &sLT)
            
            for curr in smallLeaf do
                updateNewLarge(curr, destination, &lLT)
                updateNewSmall(curr, destination, &sLT)
            
            if not isLastHop then
                nodeMailbox.Sender() <! NextPeer(next, dRT, lLT, sLT, level)

            else 
                nodeMailbox.Sender() <! Deliver(dRT, lLT, sLT)


        | NextPeer (nextPeerID, rt, lLT, sLT, level) ->
            rTable <- Array2D.copy rt
            largeLeaf <- List.map (fun x -> x) lLT
            smallLeaf <- List.map (fun x -> x) sLT
            select ("akka://FSharp/user/" + nextPeerID) system <! AddMe(nodeID, rTable, largeLeaf, smallLeaf, level)

        
        | Deliver (rt, lLT, sLT) ->
            rTable <- Array2D.copy rt
            largeLeaf <- List.map (fun x -> x) lLT
            smallLeaf <- List.map (fun x -> x) sLT

            bossActor <! Joined(nodeID)
            

        return! loop ()
    } 
    loop ()


let boss numsNodes numsReq (bossMailbox:Actor<bossMessage>) = 
    let selfActor = bossMailbox.Self
    let mutable idx:int = 1
    let mutable firstNodeID:string = "hi"
    let mutable peerList= List.empty
    let mutable count:int = 0
    let mutable terminateCount:int = 0
    let mutable totalHops: double = 0.0
    let totalRequests : int = numsNodes * numsReq
    

    let rec loop () = actor {
        let! (msg: bossMessage) = bossMailbox.Receive()
        match msg with
        | Initialize ->
            firstNodeID <- getRandomID(idx)
            let peer = spawn system firstNodeID (pastryNode firstNodeID numsReq numsNodes)
            peerList <- peerList @ [peer]
            idx <- idx + 1
            peer <! FirstJoin firstNodeID

        | Joined nid -> 
            count <- count + 1
            if count = numsNodes then  
                selfActor <! Trigger
                for p in peerList do
                    let m:string = "message!"
                    p <! StartRouting m
            else
                selfActor <! Init firstNodeID

        | Trigger ->
            for p in peerList do
                let m:string = "message!"
                p <! StartRouting m
        
        | Init nid ->
            if (idx > 1) && (idx <= numsNodes) then
                let initNodeid = getRandomID(idx)
                let peer = spawn system initNodeid (pastryNode initNodeid numsReq numsNodes)
                peerList <- peerList @ [peer]
                idx <- idx + 1
                peer <! Join nid

        | Finished nHops ->
            terminateCount <- terminateCount + 1
            totalHops <- totalHops + double(nHops)
            if terminateCount >= totalRequests then
                printfn "Finished routing."
                printfn "The average hops per route: %.3f" (totalHops / double(totalRequests))
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

    0

main ()