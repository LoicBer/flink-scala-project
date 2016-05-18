def wordCount(m : (String, String)) : Array[(String, String, Int)] = {

  val wordlist = m._2.toLowerCase.split("\\W+")
    .map(w => (m._1, w, 1) )

  return wordlist

}

wordCount(("12","hihi hihi haha"))