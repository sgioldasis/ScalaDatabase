/*
 * Tasks.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package campaigns

import scala.collection.mutable.Set
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.actors.Actor
import scala.actors.Actor._


case class mSubmitTask(t:Task)
case class mEndTask(t:Task)
case class mEndTaskName(name:String)
case class mActivateTask(t:Task)
//case class mTasksAllDone

case class Task(name:String, group:String) {

    var refKpi:Kpi = null
    var refHkpi:Hkpi = null
    var refBaseSql:String = _
    var status:String = "TODO"
    var setReqTasks:Set[Task] = Set()
    var setReqGroups:Set[String] = Set()

    def after(tasks:Task*) = {
        for (t <- tasks) setReqTasks += t
    }

    def afterGroups(groups:String*) = {
        for (g <- groups) setReqGroups += g
    }

    def canStart = {
        isToDo && setReqTasks.filter(_.notDone).isEmpty
    }

    def moveStatusActive = {
        status = "ACTIVE"
    }

    def moveStatusDone = {
        status = "DONE"
    }

    def isToDo = {
        status == "TODO"
    }

    def isDone = {
        status == "DONE"
    }

    def notDone = !isDone

}

object ts extends Actor {

//trait taskScheduler {

    private var setTasks:Set[Task] = Set()
    private var qWaiting:Queue[Task] = new Queue()
    private var countSubmitted:Int = 0
    private var countFinished:Int = 0

    private var groupsNotDone:HashMap[String,Int] = new HashMap()
    private var groupsDone:Set[String] = Set()
    private var requiredNotDone:HashMap[String,Set[String]] = new HashMap()
    private var tasksDone:Set[String] = Set()


    private def groupIsDone(g:String) = {
        groupsDone.contains(g)
    }

    private def groupIsNotDone(g:String) = {
        groupsNotDone.contains(g) && groupsNotDone(g)>0
    }

    private def requiredGroupsAreDone(t:Task) = {
        t.setReqGroups.filter(!groupIsDone(_)).isEmpty
    }

    private def taskCouldStart(t:Task) = {
        !(requiredNotDone.contains(t.name) && requiredNotDone(t.name) != Set())
    }

    private def listCanStart:List[Task] = {
        //val couldStart = setTasks.filter(_.canStart)
        val couldStart = qWaiting.filter(taskCouldStart(_))
        val canStart   = couldStart.filter(requiredGroupsAreDone(_)).toList
//                println("listCanStart.couldStart="+couldStart)
//                println("listCanStart.canStart="+canStart)
//                if (canStart == List()) {
//                    println("listCanStart.groupsDone="+groupsDone)
//                    println("listCanStart.groupsNotDone="+groupsNotDone)
//                    println("listCanStart.requiredNotDone="+requiredNotDone)
//                }
        canStart
    }

    private def canQuit = {
        (countSubmitted > 0) &&
        (countFinished == countSubmitted)
    }

    private def start(t:Task):Unit = {
        qWaiting dequeueAll (wt => wt.name == t.name)
        t.moveStatusActive
        m ! mActivateTask(t)
    }

    private def startNextTasks = {
        listCanStart map start
    }

    private def addGroupNotDone(g:String) = {
        if (groupsNotDone.contains(g)) groupsNotDone(g) += 1
        else groupsNotDone += g -> 1
    }

    private def removeGroupNotDone(g:String) = {
        val remainingGroupsNotDone = groupsNotDone(g) - 1
        if (remainingGroupsNotDone==0) {
            groupsNotDone.removeKey(g)
            groupsDone += g
        }
        else groupsNotDone(g) = remainingGroupsNotDone
    }

    private def createRequiredNotDone(t:Task) = {
        var reqTaskNames:Set[String] = Set()
        for (tx <- t.setReqTasks) if (!tasksDone.contains(tx.name)) reqTaskNames += tx.name
        requiredNotDone += t.name -> reqTaskNames
    }

    private def removeRequiredNotDone(t:Task) = {
        for (wt <- setTasks) {
            val newRequiredNotDone:Set[String] =
            if (requiredNotDone.contains(wt.name))
            requiredNotDone(wt.name) - t.name
            else Set()
            if (newRequiredNotDone.isEmpty) requiredNotDone.removeKey(wt.name)
            else requiredNotDone(wt.name) = newRequiredNotDone
        }
    }

    //
    // Public Interface
    //

    def submitTasks(tasks:Task*) = {
        for (t <- tasks) submitTask(t)
    }

    def submitTask(t:Task) = {
        println("Scheduler: Submitting "+t)
        setTasks += t
        qWaiting enqueue t
        addGroupNotDone(t.group)
        createRequiredNotDone(t)
        countSubmitted += 1
//        checkStartTask(t)
        startNextTasks
    }

    def endTask(name:String):Unit = {
        setTasks.filter(_.name == name).toList map endTask
    }

    def endTask(t:Task):Unit = {
        println("Scheduler: Ending "+t)
        t.moveStatusDone
//        qWaiting dequeueAll (wt => wt.name == t.name)
        removeRequiredNotDone(t)
        removeGroupNotDone(t.group)
        countFinished += 1
        tasksDone += t.name
        if (canQuit) m ! "END"
        else startNextTasks
    }

    //
    // Functions below will be defined at caller level
    //
//    def taskActivate(t:Task):Unit
//    def tasksAllDone():Unit


    def act() {
        loop {
            react {
                case mSubmitTask(t) => submitTask(t)
                case mEndTask(t) => endTask(t)
                case mEndTaskName(name) => endTask(name)
            }
        }
    }
}
