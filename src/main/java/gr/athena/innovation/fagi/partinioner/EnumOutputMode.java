package gr.athena.innovation.fagi.partinioner;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;

/**
 * Fused dataset mode enumeration.
 * 
 * AA_MODE: Only linked triples are handled:
 * Fused triples replace the respective ones of dataset A (the fusion output is exclusively written on A).
 * 
 * BB_MODE: Only linked triples are handled:
 * Fused triples replace the respective ones of dataset B (the fusion output is exclusively written on B).
 * 
 * AB_MODE: All triples are handled: Fused triples replace the respective ones of dataset A; 
 * Un-linked triples of dataset B are copied as-is into dataset A
 * 
 * BA_MODE: All triples are handled: Fused triples replace the respective ones of dataset B; 
 * Un-linked triples of dataset A are copied as-is into dataset B
 * 
 * A_MODE: All triples are handled: Fused triples replace the respective ones of dataset A; 
 * Fused triples are removed from dataset B, which only maintains the remaining, unlinked triples
 * 
 * B_MODE: All triples are handled: Fused triples replace the respective ones of dataset B; 
 * Fused triples are removed from dataset A, which only maintains the remaining, unlinked triples
 * 
 * L_MODE: Only linked triples are handled: Only fused triples are written in a third dataset.
 * 
 * DEFAULT: Default is L_MODE.
 * 
 * @author nkarag
 */
public enum EnumOutputMode {
    DEFAULT(0), AA_MODE(1), BB_MODE(2), AB_MODE(3), BA_MODE(4), A_MODE(5), B_MODE(6), L_MODE(7);
    
    private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(EnumOutputMode.class);
    private final int value;
    
	private EnumOutputMode(int value) {
		this.value = value;
	}

	public int getValue() {
		return this.value;
	}
	
    private static final Map<Integer, EnumOutputMode> intToTypeMap = new HashMap<>();    
	static {
		for (EnumOutputMode type : EnumOutputMode.values()) {
			intToTypeMap.put(type.value, type);
		}
	}
    
	public static EnumOutputMode fromInteger(int value) {
		EnumOutputMode type = intToTypeMap.get(value);
		if (type == null)
			return EnumOutputMode.DEFAULT;
		return type;
	}

	public static EnumOutputMode fromString(String value) {
		for (EnumOutputMode item : EnumOutputMode.values()) {
			if (item.toString().equalsIgnoreCase(value)) {
				return item;
			}
		}
		return EnumOutputMode.DEFAULT;
	}
    
    @Override
    public String toString() {
        switch(this) {
            case DEFAULT: return "DEFAULT";
            case AA_MODE: return "AA_MODE";
            case BB_MODE: return "BB_MODE";
            case AB_MODE: return "AB_MODE";
            case BA_MODE: return "BA_MODE";
            case A_MODE: return "A_MODE";
            case B_MODE: return "B_MODE";
            case L_MODE: return "L_MODE";
            default: throw new IllegalArgumentException();
        }
    }    
}
