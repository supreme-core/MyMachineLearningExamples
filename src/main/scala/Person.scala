

class Person(val name : String, val age : Integer) {

  def host(company : Company): Unit = {
    company.employees.forEach((i, p) => {
      println("Person: " + p.name + "(" + p.age + ")")
    })
  }
}
