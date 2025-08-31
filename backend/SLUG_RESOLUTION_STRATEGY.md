# Optimized Company Slug Resolution Strategy

## ✅ **Updated Based on Real Usage Patterns**

Based on your observation that it's "usually either `-group` or `-holding`" that needs to be added, the slug resolution has been optimized for Swedish company patterns.

## 🎯 **New Resolution Strategy**

### **Priority Order:**
1. **Primary Suffixes** (most common): `-group`, `-holding`  
2. **Secondary Suffixes** (less common): `-ab`, `-publ`
3. **Rare Suffixes** (fallback): `-international`, `-systems`, `-tech`

### **Smart Resolution Logic:**

#### **Case 1: Company slug has no suffix**
Example: `absolent-air-care`
```
Testing order:
1. absolent-air-care-group     ← Most likely
2. absolent-air-care-holding   ← Second most likely  
3. absolent-air-care-ab        ← Less common
4. absolent-air-care-publ      ← Least common
```

#### **Case 2: Company slug has wrong suffix**  
Example: `embracer-ab` (but real URL is `embracer-group`)
```
Testing order:
1. embracer-group              ← Try primary alternatives
2. embracer-holding            ← Try other primary
3. embracer                    ← Try without suffix
```

## 📊 **Expected Results**

### **Companies Likely to Be Fixed:**
```
absolent-air-care      → absolent-air-care-group
kinnevik               → kinnevik-group OR kinnevik-holding  
lagercrantz            → lagercrantz-group
investor               → investor-ab (special case)
embracer               → embracer-group
yubico                 → yubico-ab (secondary suffix)
```

### **Performance Improvements:**
- **Faster Resolution**: Only 4 attempts max (vs 7 before)
- **Higher Success Rate**: Tests most common patterns first
- **Respectful Rate Limiting**: 0.5s between attempts (2 seconds total max)

## 🔍 **Log Messages You'll See:**

```
🔄 404 for absolent-air-care, attempting slug resolution...
🔍 Resolving slug variations for: absolent-air-care
   📝 Testing variations: ['absolent-air-care-group', 'absolent-air-care-holding', 'absolent-air-care-ab', 'absolent-air-care-publ']
   ❌ absolent-air-care-group returned 200
   ✅ Found working variation: absolent-air-care-group
🎯 Retrying with resolved slug: absolent-air-care-group
```

## 🚀 **Integration**

The optimized strategy is now built into:
- ✅ `mfn_collector.py` (lines 181-213)
- ✅ Historical ingestion batch processor  
- ✅ Swedish document ingestor worker
- ✅ All MFN-based collection workflows

## 🧪 **Test Companies**

Updated `temp_companies.txt` with companies most likely to need `-group` or `-holding`:

```
absolent-air-care     → likely needs -group
kinnevik              → likely needs -group or -holding  
lagercrantz           → likely needs -group
embracer              → likely needs -group
investor              → special case (might need -ab)
```

This targeted approach should resolve the "bad results" much more efficiently by focusing on the actual patterns you've observed in Swedish company URLs! 🎯