from header import main as header_main
from read_s3 import main as read_S3_main
from write_postgre_to_s3 import main as write_postgre_to_s3_main
from read_postgre import main as read_postgre_main
from write_s3_to_postgre import main as write_s3_to_postgre_main

def main():
    header_main()
    while True:
        print(" \n Choose an option and press enter:\n")
        print("  1 - Read from S3.")
        print("  2 - Write from PostgreSQL to S3.")
        print("  3 - Read from PostgreSQL.")
        print("  4 - Write from S3 to PostgreSQL.")
        print("  5 - Exit.")
        
        opcao = input()
        
        if opcao == '1':
            try:
                read_S3_main()
            except Exception as e:
                print(" \nError when executing write_postgres_To_S3. Check your credentials and connections. Error:\n", e)
        elif opcao == '2':
            try:
                write_postgre_to_s3_main()
            except Exception as e:
                print(" \nError when executing write_postgres_To_S3. Check your credentials and connections. Error:\n", e)
        elif opcao == '3':
            try:
                read_postgre_main()
            except Exception as e:
                print(" \nError when executing read_Redshift. Check your credentials and connections. Error:\n", e)
        elif opcao == '4':
            try:
                write_s3_to_postgre_main()
            except Exception as e:
                print(" \nError when executing write_S3_To_Redshift. Check your credentials and connections. Error:\n", e)
        elif opcao == '5':
            break
        else:
            print(" \nInvalid option. Choose again.\n")

if __name__ == "__main__":
    main()
