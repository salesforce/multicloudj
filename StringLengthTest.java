public class StringLengthTest {
    public static void main(String[] args) {
        String small = "JohnBenchmarkSmall1";
        String medium = "JohnBenchmarkMedium1_ExtraDataForMediumSizeTest";
        String large = "JohnBenchmarkLarge1_ExtraDataForLargeSizeTestWithMuchLongerStringContent_AdditionalPadding";
        
        System.out.println("Small player name: \"" + small + "\"");
        System.out.println("Length: " + small.length() + " characters");
        System.out.println();
        
        System.out.println("Medium player name: \"" + medium + "\"");
        System.out.println("Length: " + medium.length() + " characters");
        System.out.println();
        
        System.out.println("Large player name: \"" + large + "\"");
        System.out.println("Length: " + large.length() + " characters");
        System.out.println();
        
        System.out.println("The length determines the 'baseValue' passed to createPlayer()");
        System.out.println("createPlayer() then uses this length to create different sized data");
    }
} 