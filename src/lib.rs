pub mod pager;

pub mod test {
    pub use crate::pager::Pager;

    #[test]
    fn test_single_span() {
        let mut pager = Pager::new("test.db").unwrap();
        let mut document = pager.doc();
        let id = document.id();

        document
            .content
            .insert("hello".to_string(), "world".to_string());

        document
            .content
            .insert("foo".to_string(), "bar".to_string());

        document
            .content
            .insert("baz".to_string(), "qux".to_string());

        pager.write_document(document).unwrap();

        let document = pager.read_document(id).unwrap();

        assert_eq!(document.content.get("hello"), Some(&"world".to_string()));
        assert_eq!(document.content.get("foo"), Some(&"bar".to_string()));
        assert_eq!(document.content.get("baz"), Some(&"qux".to_string()));

        println!("[+] Wrote first doc!");

        let mut document = pager.doc();
        let id = document.id();

        document
            .content
            .insert("hello".to_string(), "world".to_string());

        document
            .content
            .insert("foo".to_string(), "bar".to_string());

        document
            .content
            .insert("baz".to_string(), "qux".to_string());

        pager.write_document(document).unwrap();

        let document = pager.read_document(id).unwrap();

        assert_eq!(document.content.get("hello"), Some(&"world".to_string()));
        assert_eq!(document.content.get("foo"), Some(&"bar".to_string()));
        assert_eq!(document.content.get("baz"), Some(&"qux".to_string()));

        println!("[+] Wrote second doc!");
    }

    #[test]
    pub fn test_multi_spans() {
        let mut pager = Pager::new("test.db").unwrap();
        let mut document = pager.doc();
        let id = document.id();

        document
            .content
            .insert("hello".to_string(), "world".to_string());

        document
            .content
            .insert("foo".to_string(), "bar".to_string());

        document
            .content
            .insert("baz".to_string(), "qux".to_string());

        pager.write_document(document).unwrap();

        let document = pager.read_document(id).unwrap();

        assert_eq!(document.content.get("hello"), Some(&"world".to_string()));
        assert_eq!(document.content.get("foo"), Some(&"bar".to_string()));
        assert_eq!(document.content.get("baz"), Some(&"qux".to_string()));

        println!("[+] Wrote first doc!");
    }
}
