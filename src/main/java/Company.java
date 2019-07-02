import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class Company {

    HashMap<Integer, Person> employees = new HashMap<>(Map.of(
            1, new Person("John", 12),
            2, new Person("Shakira", 32),
            3, new Person("Rihanna", 56),
            4, new Person("Sabby", 41)
    ));

    AtomicInteger lastId = new AtomicInteger(employees.size() -1);

    public void add(Person p) {
        employees.put(lastId.incrementAndGet(), p);
    }

    public void insert(String name, Integer age) {
        employees.put(lastId.incrementAndGet(), new Person(name, age));
    }

    public void update(Integer id, String name, Integer age) {
        Person p = employees.get(id);
        employees.replace(id, p, new Person(name, age));
    }

    public void delete(Integer id) {
        if (employees.containsKey(id)) {
            employees.remove(id);
        }
    }
}
