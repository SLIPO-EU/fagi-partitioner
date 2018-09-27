package gr.athena.innovation.fagi.partinioner;

/**
 * Wrapper exception for wrong input related exceptions.
 * 
 * @author nkarag
 */
public class WrongInputException extends Exception{

      public WrongInputException() {}

      public WrongInputException(String message){
         super(message);
      }
}

