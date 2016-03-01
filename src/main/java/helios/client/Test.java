package helios.client;

/**
 * @Project: helios
 * @Title: Test.java
 * @Package helios.client
 * @Description: TODO
 * @author YuesongWang
 * @date Mar 1, 2016 12:42:21 AM
 * @version V1.0
 */
public class Test {
    /** 
     * Description: TODO
     * @param test
     * void
     */
    public static void fuck(Test test) {
        test.fuck();
    }

    public String name;
    public Test(String name){
        this.name = name;
    }
    
    public void fuck(){
        
    }
    
    public void shit(){
        Test.fuck(this);
    }
//    public static class TestBuilder {
//        public String name;
//
//        public TestBuilder() {
//
//        }
//
//        public TestBuilder withName(String name) {
//            this.name = name;
//            return this;
//        }
//        
//        public Test build(){
//            return new Test(this);
//        }
//    }
//    
//    public Test(TestBuilder b){
//        this.name = b.name;
//    }
//    
// 
//    
//    public static void main(String[] args){
//        Test t =  new TestBuilder().withName("Shit").build();
//        System.out.println(t.name);
//    }
}
