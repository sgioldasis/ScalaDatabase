/*
 * Dimensions.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package campaigns

/*
 * Main.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */


import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set


case class Path(path:String) {
    val allParts = List.fromString(path, '\\')
    val lastPart = allParts.last
    val agrParts = allParts filter (_ != lastPart)
}

case class DimMember(objId:String, objPath:String) {

    val id           = objId
    val path         = Path(objPath)

    val pathAllParts = path.allParts
    val pathLastPart = path.lastPart
    val pathAgrParts = path.agrParts

    var isBase       = true
    var isValid      = true


}

case class Dim(name:String) {

    var setAll = new HashSet[String]
    var setBas = new HashSet[String]
    var setAgr = new HashSet[String]

    private val mapAll    = new HashMap[String, DimMember]
    private val mapTmpAgr = new HashMap[String, Set[String]]()
    private val mapTmpSunits = new HashMap[String, Set[String]]()

    var listAll   = setAll.toList
    var listBas   = setBas.toList
    var listAgr   = setAgr.toList
    var mapAgr    = new HashMap[String, List[String]]()
    var mapSunits = new HashMap[String, Set[String]]()


    def mergeKey(x:String, y:String): String = {
        x + "." + y
    }

//    private def key(args:String*): String = {
//        var key = ""
//        for (arg <- args) key = key + arg + "."
//        "[" + key + "]"
//    }

    

    // Method [merge]
    //
    //        Merges a new dimension to the current one
    //
    def merge(newDim:Dim): Dim = {

        val result = new Dim(name+"."+newDim.name)

        for (x <- setAll ; y <- newDim.setAll) {
            result.setAll += mergeKey(x,y)
        }

        for (x <- setBas ; y <- newDim.setBas) {
            result.setBas += mergeKey(x,y)
        }

        // Create setAgr, mapTmpAgr for x.bas * y.agr
        for (x <- setBas ; y <- newDim.setAgr) {
            // Create xy entry in result setAgr
            val xy = mergeKey(x,y)
            result.setAgr += xy

            // Create xy entry in result mapTmpAgr
            if (newDim.mapAgr.contains(y))
            for( agry <- newDim.mapAgr(y)) {
                val agrxy = mergeKey(x,agry)
                if (result.mapTmpAgr.contains(xy))
                result.mapTmpAgr(xy) += agrxy
                else
                result.mapTmpAgr += (xy -> Set(agrxy))
            }
        }

        // Create setAgr, mapTmpAgr for x.agr * y.bas
        for (x <- setAgr ; y <- newDim.setBas) {
            // Create xy entry in result setAgr
            val xy = mergeKey(x,y)
            result.setAgr += xy

            // Create xy entry in result mapTmpAgr
            if (mapAgr.contains(x))
            for( agrx <- mapAgr(x)) {
                val agrxy = mergeKey(agrx,y)
                if (result.mapTmpAgr.contains(xy))
                result.mapTmpAgr(xy) += agrxy
                else
                result.mapTmpAgr += (xy -> Set(agrxy))
            }
        }
        
        // Create setAgr, mapTmpAgr for x.agr * y.agr
        for (x <- setAgr ; y <- newDim.setAgr) {
            // Create xy entry in result setAgr
            val xy = mergeKey(x,y)
            result.setAgr += xy

            // Create xy entry in result mapTmpAgr
            if (mapAgr.contains(x) && newDim.mapAgr.contains(y))
            for( agrx <- mapAgr(x) ; agry <- newDim.mapAgr(y) ) {
                val agrxy = mergeKey(agrx,agry)
                if (result.mapTmpAgr.contains(xy))
                result.mapTmpAgr(xy) += agrxy
                else
                result.mapTmpAgr += (xy -> Set(agrxy))
            }
        }


        // Convert sets to lists for easier use
        result.listAll = result.setAll.toList.sort(_<_)
        result.listBas = result.setBas.toList.sort(_<_)
        result.listAgr = result.setAgr.toList.sort(_<_)

        // Create mapAgr such that
        // it contains only keys found in listAgr
        for (k <- result.mapTmpAgr.keys) {
            if (result.setAgr.contains(k)) result.mapAgr += k -> result.mapTmpAgr(k).toList
        }

        result
    }

    // Method [add]
    //
    //        Adds a new key to the dimension
    //
    def add(newKey:DimMember): Unit = {

        // Only add the key if it is valid
        if (newKey.isValid) {

            // Add key to set of all keys
            setAll += newKey.id

            // Add key to the map of all keys
            mapAll += newKey.id -> newKey

            // If the key is a base key
            if (newKey.isBase) {

                // Add the key to the set of base keys
                setBas += newKey.id

                // For each key in the aggregate parts of the key's path
                newKey.pathAgrParts foreach { agrKey =>

                    // If there is already an entry for the aggregate key
                    // then update the aggregate entry set with the new key
                    if (mapTmpAgr.contains(agrKey))
                    mapTmpAgr(agrKey) += newKey.id
                    // else create an aggregate entry set
                    // containing only the new key
                    else
                    mapTmpAgr += (agrKey -> Set(newKey.id))
                }
            } 
            
            // If the key is an aggregate key
            else {

                // Just add the key to the set of aggregate keys
                setAgr += newKey.id
            }

            // Determine Sunits
            newKey.pathAgrParts foreach { agrKey =>
                if (mapTmpSunits.contains(agrKey))
                mapTmpSunits(agrKey) += newKey.id
                else
                mapTmpSunits += (agrKey -> Set(newKey.id))
            }

            
            // Convert sets to lists for easier use
            listAll = setAll.toList
            listBas = setBas.toList
            listAgr = setAgr.toList

            // Create mapAgr such that
            // it contains only keys found in listAgr
            for (k <- mapTmpAgr.keys) {
                if (setAgr.contains(k)) mapAgr += k -> mapTmpAgr(k).toList
            }

            // Create mapSunits such that
            // it contains only keys found in listAgr
            for (k <- mapTmpSunits.keys) {
                //if (setAgr.contains(k))
                mapSunits += k -> mapTmpSunits(k)
            }

        }

        
    }

}


case class Segment(segmentId:String,
                   codSegment:String,
                   chrSegment:String,
                   codLevelSegment:Int,
                   codCustType:String,
                   segmentPath:String) {

    val dm = new DimMember(segmentId,segmentPath)

    dm.isBase  = codLevelSegment == 2

}



