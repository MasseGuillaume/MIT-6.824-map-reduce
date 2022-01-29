val elements = (1 to 20).toList

val groups = 3


val groupSize = elements.size / groups

val grouping = elements.grouped(groupSize).toList

grouping.foreach(println)


val (list, tail) = grouping.splitAt(groups)

list
tail

list.zipAll(tail, Nil, Nil).map {
  case (h, t) => h ++ t
}

