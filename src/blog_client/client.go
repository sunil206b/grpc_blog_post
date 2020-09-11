package main

import (
	"context"
	"fmt"
	"github.com/sunil206b/grpc_blog_post/src/blogpb"
	"google.golang.org/grpc"
	"io"
	"log"
)

func main() {
	// If we crash the go code, we get the file name and line number
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	fmt.Println("Blog Client...")

	cc, err := grpc.Dial("0.0.0.0:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect: %v\n", err)
		return
	}
	defer cc.Close()

	c := blogpb.NewBlogServiceClient(cc)

	blog := &blogpb.CreateBlogRequest{
		Blog: &blogpb.Blog{
			AuthorId: "Duplicate Data",
			Title: "This is for testing delete functionality",
			Content: "This is for testing delete functionality",
		},
	}
	res, err := c.CreateBlog(context.Background(), blog)
	if err != nil {
		log.Fatalf("Failed to create the blog post %v\n", err)
		return
	}
	tempBlogId := res.GetBlog().GetId()
	fmt.Printf("Blog has beeen created: %v\n", res.GetBlog())

	fmt.Println("Reading the blog...")
	blog1, err := c.GetBlog(context.Background(), &blogpb.GetBlogRequest{
		BlogId: "5f5ac3c80a6a1d94c129475a",
	})
	if err != nil {
		log.Fatalf("Error when reading blog: %v\n", err)
		return
	}
	fmt.Printf("Reading blog from the server: %v\n", blog1.GetBlog())

	fmt.Println("Updating the blog...")

	newBlog := &blogpb.UpdateBlogRequest{
		Blog: &blogpb.Blog{
			Id:  "5f5acaee2b854ec87b944efd",
			AuthorId: "Virendar Sehwag",
			Title: "How to become hard hitter",
			Content: "This will give techniques required to play hard hitting in Cricket and make great scores",
		},
	}

	updateRes, err := c.UpdateBlog(context.Background(), newBlog)
	if err != nil {
		log.Fatalf("Failed to update blog: %v\n", err)
		return
	}
	fmt.Printf("Updated blog post: %v\n", updateRes.GetBlog())

	fmt.Println("Deleting blog post...")
	deleteRes, err := c.DeleteBlog(context.Background(), &blogpb.DeleteBlogRequest{
		BlogId: tempBlogId,
	})
	if err != nil {
		log.Fatalf("Failed to delete blog: %v\n", err)
		return
	}
	fmt.Printf("Blog post deleted successfully: %v\n", deleteRes.GetBlogId())

	fmt.Println("Getting all the blog posts...")
	stream, err := c.ListBlog(context.Background(), &blogpb.ListBlogRequest{})
	if err != nil {
		log.Fatalf("Error while calling ListBlog RPC: %v\n", err)
		return
	}

	for  {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Something happened: %v\n", err)
			return
		}
		fmt.Printf("List og Blogs %v\n", res.GetBlog())
	}
}
