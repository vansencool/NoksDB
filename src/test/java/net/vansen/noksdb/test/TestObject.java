package net.vansen.noksdb.test;

public class TestObject {

    public String name;
    public int age;

    public TestObject(String name, int age) {
        this.age = age;
        this.name = name;
    }

    public TestObject setName(String name) {
        this.name = name;
        return this;
    }

    public TestObject setAge(int age) {
        this.age = age;
        return this;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    @Override
    public String toString() {
        return "TestObject{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
