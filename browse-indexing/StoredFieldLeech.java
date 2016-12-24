// Build a browse list by walking the docs in an index and extracting sort key
// and values from a pair of stored fields.

import java.io.*;
import java.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;

import org.vufind.util.Utils;
import org.vufind.util.BrowseEntry;

public class StoredFieldLeech extends Leech
{
    int currentDoc = 0;
    LinkedList<BrowseEntry> buffer;

    String sortField;
    String valueField;

    private Set<String> fieldSelection;


    public StoredFieldLeech (String indexPath, String field, String prependFromField) throws Exception
    {
        super (indexPath, field, prependFromField);

        sortField = Utils.getEnvironment ("SORTFIELD");
        valueField = Utils.getEnvironment ("VALUEFIELD");

        if (sortField == null || valueField == null) {
            throw new IllegalArgumentException ("Both SORTFIELD and " +
                                                "VALUEFIELD environment " +
                                                "variables must be set.");
        }

        fieldSelection = new HashSet<String>();
        fieldSelection.add(sortField);
        fieldSelection.add(valueField);
        fieldSelection.add("id");   // make Solr id available for error messages

        // optional field used to filter results from the complete index, e.g., location, institution
        if (this.prependFromField != null) {
            fieldSelection.add(prependFromField);
        }

        reader = DirectoryReader.open (FSDirectory.open (new File (indexPath).toPath ()));
        buffer = new LinkedList<BrowseEntry> ();
    }


    private void loadDocument (IndexReader reader, int docid)
        throws Exception
    {
        Document doc = reader.document (currentDoc, fieldSelection);

        // optional field used to filter results from the complete index, e.g., location, institution
        // we will prepend the value of this field to the sort_key (un-normalized) of our headings
        // so that later we can sort and filter our results based on a particular value for this field
        String prependFromValue = "";
        if (this.prependFromField != null) {
            String[] prependValues = doc.getValues (this.prependFromField);
            if (prependValues.length > 0) {
                prependFromValue = prependValues[0];
            }
            prependFromValue += FROM_FIELD_DELIMITER; // always need to add this delimiter
        }
        String[] sort_key = doc.getValues (sortField);
        String[] value = doc.getValues (valueField);

        if (sort_key.length == value.length) {
            for (int i = 0; i < value.length; i++) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                outputStream.write(prependFromValue.getBytes()); // can be empty if this.prependField is null; that's okay
                outputStream.write(buildSortKey(sort_key[i]));
                byte prepended_normalized_sort_key[] = outputStream.toByteArray();
                buffer.add (new BrowseEntry(prepended_normalized_sort_key,
                                            sort_key[i],
                                            value[i]));
            }
        } else {
            String id = null;
            IndexableField idField = doc.getField("id");
            if (idField != null) {
                /*
                 * Assumes id is defined as type string in Solr schema.
                 * Should be safe for VuFind.
                 */
                id = idField.stringValue();
            }
            System.err.println("Skipped entries for doc #" + docid +
                               " (id:" + id + "):" +
                               " the number of sort keys didn't" +
                               " match the number of stored values.");
        }
    }


    public BrowseEntry next() throws Exception
    {
        while (buffer.isEmpty ()) {
            if (currentDoc < reader.maxDoc ()) {
                loadDocument (reader, currentDoc);
                currentDoc++;
            } else {
                return null;
            }
        }

        return buffer.remove ();
    }
}

